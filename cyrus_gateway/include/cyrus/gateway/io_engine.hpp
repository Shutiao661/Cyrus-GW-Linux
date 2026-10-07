// ============================================================================
// io_engine.hpp - 异步 I/O 引擎抽象接口
// ============================================================================
// 定义异步 I/O 操作接口。io_uring 采用 "投递操作 → 接收完成通知" 模型,
// 因此抽象为:
//   1. post_xxx()  → 投递异步操作 (非阻塞, 立即返回)
//   2. wait_completions() → 等待完成事件 (阻塞直到有事件到达)
//   3. 完成回调 → 根据 IOContext 中的操作类型分发处理
//
// 具体实现:
//   - IOEngineUring (Linux): 使用 io_uring (liburing 不可用时回退 POSIX)
// ============================================================================

#pragma once

#include "cyrus/types.hpp"

#include <netinet/in.h>  // sockaddr_in (Accept 完成时记录对端地址)
#include <functional>
#include <memory>

namespace cyrus {
namespace gateway {

// ============================================================================
// IOOperation - 异步操作类型
// ============================================================================
enum class IOOperation : uint8_t {
    NONE   = 0,    // 未初始化
    ACCEPT = 1,    // 接受新连接 (仅监听 socket)
    RECV   = 2,    // 接收数据
    SEND   = 3,    // 发送数据
};

// ============================================================================
// IOContext - 异步操作上下文
// ============================================================================
// 每个异步操作对应一个 IOContext。
// 投递操作时填充此结构 → 完成时 I/O 引擎填充结果字段。
//
// 生命周期: IOContext 必须在操作进行期间保持有效。
// 通常从对象池分配, 完成处理后归还。
struct IOContext {
    IOOperation  op = IOOperation::NONE;  // 操作类型
    socket_t     fd = INVALID_SOCKET_VAL; // 操作的套接字
    socket_t     accept_fd = INVALID_SOCKET_VAL;  // Accept 完成: 新连接的 socket
    uint8_t*     buffer = nullptr;        // 数据缓冲区 (RECV: 接收缓冲区, SEND: 发送缓冲区)
    size_t       buffer_len = 0;          // 缓冲区大小
    size_t       bytes_transferred = 0;   // 完成时: 实际传输的字节数
    int          error = 0;               // 完成时: 0=成功, 非0=错误码
    void*        user_data = nullptr;      // 用户数据 (通常指向 Connection 对象)

    // 完成回调 (协程路径专用)
    //   非空: wait_completions() 填好 bytes_transferred/error 后直接回调,
    //         由回调恢复 (resume) 挂起的协程, 该上下文不再返回给上层分发。
    //   为空: 传统路径 —— 上下文返回给调用者 (Server 按 op 类型分发)。
    // 用裸函数指针而非 std::function: 回调在每次 I/O 完成时都要走一遍,
    // 不能引入堆分配。
    void (*on_complete)(IOContext*) = nullptr;

    // Accept 完成: 对端客户端地址 (用于 per-IP 限流等)
    sockaddr_in  accept_addr{};
    socklen_t    accept_addr_len = sizeof(sockaddr_in);

    // 重置上下文 (归还池前清空)
    void reset() {
        op = IOOperation::NONE;
        fd = INVALID_SOCKET_VAL;
        accept_fd = INVALID_SOCKET_VAL;
        buffer = nullptr;
        buffer_len = 0;
        bytes_transferred = 0;
        error = 0;
        user_data = nullptr;
        on_complete = nullptr;
        accept_addr = sockaddr_in{};
        accept_addr_len = sizeof(sockaddr_in);
    }
};

// ============================================================================
// IOEngine - 异步 I/O 引擎抽象基类
// ============================================================================
class IOEngine {
public:
    virtual ~IOEngine() = default;

    // --- 生命周期 ---

    // 初始化引擎 (创建 io_uring 队列等)
    // 返回 true 表示成功
    virtual bool init() = 0;

    // 关闭引擎 (释放资源, 取消所有挂起的操作)
    virtual void shutdown() = 0;

    // --- Socket 注册 ---

    // 将 socket 注册到引擎
    // fd: 套接字
    // user_data: 与此 socket 关联的用户数据 (通常是 Connection*)
    virtual bool register_socket(socket_t fd, void* user_data) = 0;

    // --- 投递异步操作 ---

    // 投递异步 Accept 操作
    // listen_fd: 监听套接字
    // ctx: 操作上下文 (ctx->fd 将被设置为新接受的 socket)
    // 返回 0 成功, -1 失败
    virtual int post_accept(socket_t listen_fd, IOContext* ctx) = 0;

    // 投递异步 Recv 操作
    // fd: 已连接的套接字
    // ctx: 操作上下文 (ctx->buffer 必须已填充缓冲区指针)
    // 返回 0 成功, -1 失败
    virtual int post_recv(socket_t fd, IOContext* ctx) = 0;

    // 投递异步 Send 操作
    // fd: 已连接的套接字
    // ctx: 操作上下文 (ctx->buffer 指向要发送的数据, ctx->bytes_transferred = 待发送长度)
    // 返回 0 成功, -1 失败
    virtual int post_send(socket_t fd, IOContext* ctx) = 0;

    // --- 等待完成事件 ---

    // 等待完成事件 (阻塞)
    // contexts: 输出数组, 引擎填充完成事件的 IOContext 指针
    // max_events: 最多返回的事件数
    // timeout_ms: 超时 (毫秒), -1 = 无限等待
    // 返回实际完成的事件数 (0 = 超时)
    virtual int wait_completions(IOContext** contexts, int max_events,
                                  int timeout_ms) = 0;

    // --- 唤醒 (用于优雅关闭) ---

    // 向完成队列投递一个特殊的 "wakeup" 事件
    // 用于在 shutdown 时唤醒阻塞在 wait_completions() 上的工作线程
    virtual void post_wakeup() = 0;

    // --- Context Pool ---
    // 从对象池获取/归还 IOContext。具体实现由子类提供:
    //   IOEngineUring → UringContext 池
    virtual IOContext* acquire_context() = 0;
    virtual void release_context(IOContext* ctx) = 0;
};

// ============================================================================
// 引擎工厂函数
// ============================================================================

// 创建 I/O 引擎 (io_uring, liburing 不可用时回退 POSIX)
std::unique_ptr<IOEngine> create_io_engine();

} // namespace gateway
} // namespace cyrus
