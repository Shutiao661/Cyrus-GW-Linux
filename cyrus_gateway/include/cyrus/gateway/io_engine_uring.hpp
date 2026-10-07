// ============================================================================
// io_engine_uring.hpp - Linux io_uring I/O 引擎
// ============================================================================
// 基于 io_uring 的高性能异步 I/O。liburing 不可用时回退 POSIX (同步 I/O)。
//
// io_uring 核心原理:
//   - SQ (Submission Queue): 用户态→内核态, 环形缓冲区, 提交 I/O 请求
//   - CQ (Completion Queue): 内核态→用户态, 环形缓冲区, 返回 I/O 结果
//   - 两个队列通过共享内存映射, 避免系统调用
//   - 支持批量提交 (多个 SQE 一次 enter) 和批量收割 (多个 CQE 一次 peek)
// ============================================================================

#pragma once

#include "io_engine.hpp"
#include "cyrus/logger.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <deque>
#include <condition_variable>
#if CYRUS_HAS_LIBURING
#include <liburing.h>
#endif

namespace cyrus {
namespace gateway {

// ============================================================================
// UringContext - io_uring 特定的操作上下文
// ============================================================================
struct UringContext : public IOContext {
    // io_uring 使用 user_data 字段关联操作
    // SQE 的 user_data 被设置为 UringContext*,
    // CQE 的 user_data 返回同样的值, 用于匹配

    // 操作类型编码 (高 8 位) + UringContext 指针 (低 56 位)
    static uint64_t encode(IOOperation op, UringContext* ctx) {
        return (static_cast<uint64_t>(static_cast<uint8_t>(op)) << 56)
             | (reinterpret_cast<uint64_t>(ctx) & 0x00FFFFFFFFFFFFFFULL);
    }

    static IOOperation decode_op(uint64_t user_data) {
        return static_cast<IOOperation>(static_cast<uint8_t>(user_data >> 56));
    }

    static UringContext* decode_ctx(uint64_t user_data) {
        return reinterpret_cast<UringContext*>(user_data & 0x00FFFFFFFFFFFFFFULL);
    }
};

// ============================================================================
// IOEngineUring - Linux io_uring 引擎
// ============================================================================
class IOEngineUring : public IOEngine {
public:
    IOEngineUring() = default;
    ~IOEngineUring() override;

    // --- IOEngine 接口 ---
    bool init() override;
    void shutdown() override;
    bool register_socket(socket_t fd, void* user_data) override;

    int post_accept(socket_t listen_fd, IOContext* ctx) override;
    int post_recv(socket_t fd, IOContext* ctx) override;
    int post_send(socket_t fd, IOContext* ctx) override;

    int wait_completions(IOContext** contexts, int max_events,
                        int timeout_ms) override;

    void post_wakeup() override;

    IOContext* acquire_context() override;
    void release_context(IOContext* ctx) override;

private:
    bool initialized_ = false;
#if CYRUS_HAS_LIBURING
    struct io_uring ring_;
#endif
    std::atomic<bool> shutting_down_{false};

    // SQ/CQ 串行化锁。
    // liburing 的 io_uring_get_sqe()/io_uring_submit() 不是线程安全的: SQ 队尾
    // 是非原子的, 两个 worker 并发提交会拿到同一个 SQE 槽, 造成 SQE 被覆盖、
    // 完成事件与操作错配 (实测表现为 accept 完成里 decode 出别的操作类型、
    // 以及 ctx 被重复回收引发的 use-after-free)。
    // 因此提交与收割都必须串行化。阻塞等待 (io_uring_wait_cqe) 不推进 CQ 队头,
    // 放在锁外做, 否则一个 worker 阻塞会让其他线程无法提交。
    // 注: 更彻底的做法是每线程独立 ring (IORING_SETUP_SQPOLL/ATTACH_WQ),
    // 这里先用锁保证正确性。
    std::mutex ring_mutex_;

    // UringContext 对象池
    std::vector<std::unique_ptr<UringContext>> context_pool_;
    std::vector<UringContext*> free_contexts_;
    std::mutex pool_mutex_;

    // POSIX fallback (CYRUS_HAS_LIBURING=0 时使用)
    std::thread accept_thread_;
    bool accept_thread_running_ = false;
    socket_t listen_fd_ = INVALID_SOCKET_VAL;
    std::deque<IOContext*> completion_queue_;
    std::mutex completion_mutex_;
    std::condition_variable completion_cv_;
};

} // namespace gateway
} // namespace cyrus
