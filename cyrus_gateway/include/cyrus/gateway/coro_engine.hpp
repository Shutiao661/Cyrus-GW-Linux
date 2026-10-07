// ============================================================================
// coro_engine.hpp - C++20 协程适配层 (Linux io_uring)
// ============================================================================
// 为 io_uring I/O 引擎提供 C++20 协程接口, 简化异步编程模型。
//
// 使用 C++20 协程的三个关键类型:
//   - promise_type: 控制协程生命周期
//   - Awaitable: 定义 co_await 行为
//   - Task<T>: 协程返回值类型
//
// 工作原理 (完成事件 → 协程恢复的完整链路):
//   1. co_await async_recv(fd, buf) 构造 awaiter, 其内部持有一个 UringContext
//   2. await_suspend() 填充 ctx 并调用 engine->post_recv() 投递 SQE, 协程挂起
//      —— 引擎把 ctx 指针编码进 sqe->user_data (高 8 位操作类型 + 低 56 位指针)
//   3. CQE 返回时 wait_completions() 解码出 ctx, 填入 bytes_transferred/error,
//      发现 ctx->on_complete 非空 → 调用回调
//   4. 回调经 ctx->user_data 找回 awaiter, resume 协程, 从 await_resume() 继续
//
// 恢复发生在哪个线程? (关键设计决策)
//   resume 就在"收割到该 CQE 的那个 I/O worker 线程"上, 不做跨线程投递。
//   理由:
//     - 零交接成本: 投递到别的队列/线程意味着每次 I/O 多一次入队 + 唤醒,
//       这正是 io_uring 想避免的开销;
//     - 协程帧只被一个线程访问, 状态无需加锁;
//     - 与 io_uring 的线程模型一致 (每个 worker 各自收割自己的 ring)。
//   代价与约束:
//     - 协程体里不能长时间占用 CPU, 否则该 worker 收割其他完成事件会被拖住;
//       因此协程必须在每个 I/O 边界让出 (本项目解析量有 max_header_size /
//       max_body_size 上限, 属于有界工作)。
//     - 若将来需要 CPU 密集的协程体, 应改为把 resume 投递到计算线程池,
//       那是另一种取舍, 不是这里的默认选择。
//
// 平台限制:
//   - Linux + GCC 10+/Clang 14+: 完整支持
//   - 无 liburing 时 (POSIX fallback) post_recv/post_send 是同步阻塞的,
//     协程语义退化为"阻塞 + 挂起", 链路仍然正确但不再有异步收益;
//     真正的异步收益只在 io_uring 路径上。
// ============================================================================

#pragma once

#include "io_engine_uring.hpp"

#include <coroutine>
#include <exception>
#include <functional>
#include <optional>

namespace cyrus {
namespace gateway {
namespace coro {

// ============================================================================
// Task<T> - 协程返回值 (惰性求值, 仅在 co_await 或 detach 时启动)
// ============================================================================
// 生命周期规则 (协程帧最容易踩的坑):
//   - 默认由 Task 对象持有: final_suspend 处挂起, 由 ~Task() 销毁帧
//     (因此 Task 不能是临时对象就丢掉 —— 那样帧永远不销毁).
//   - detach():  fire-and-forget。交出所有权, 帧在 final_suspend 处自毁
//     (由 promise 里的 detached_ 标记决定), 不会泄漏。
//   - co_await task: 当前协程挂起, 被等待的 Task 启动; 它跑完后通过
//     continuation_ 恢复等待者 (symmetric transfer, 不额外压栈)。
// ============================================================================
template <typename T = void>
class Task {
public:
    struct promise_type {
        T result_;
        std::exception_ptr exception_;
        std::coroutine_handle<> continuation_ = nullptr;  // 等待此 Task 的协程
        bool detached_ = false;                           // 是否已交出所有权

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept { return {}; }

        // 结束时的去向: 有等待者 → 交还控制权; 已 detach → 自毁帧; 否则挂起等 ~Task
        struct FinalAwaiter {
            bool await_ready() noexcept { return false; }
            std::coroutine_handle<> await_suspend(
                std::coroutine_handle<promise_type> h) noexcept
            {
                auto& p = h.promise();
                if (p.continuation_) {
                    auto cont = p.continuation_;
                    p.continuation_ = nullptr;
                    return cont;                       // 对称转移到等待者
                }
                if (p.detached_) {
                    h.destroy();                       // 无人持有 → 自行销毁
                }
                return std::noop_coroutine();
            }
            void await_resume() noexcept {}
        };
        FinalAwaiter final_suspend() noexcept { return {}; }

        void return_value(T value) { result_ = std::move(value); }
        void unhandled_exception() { exception_ = std::current_exception(); }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    explicit Task(handle_type h) : handle_(h) {}
    ~Task() { if (handle_) handle_.destroy(); }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&& other) noexcept : handle_(other.handle_) { other.handle_ = nullptr; }

    // 获取结果
    T result() const {
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        return std::move(handle_.promise().result_);
    }

    // 协程句柄
    auto handle() { return handle_; }

    // 启动并放弃所有权 (fire-and-forget)。
    // 帧在协程跑完的 final_suspend 处自行销毁; 此后不得再访问本 Task。
    void detach() {
        auto h = handle_;
        handle_ = nullptr;
        if (h) {
            h.promise().detached_ = true;
            h.resume();          // 启动; 若同步跑完, 帧已被销毁
        }
    }

    // Awaitable: 允许 co_await task
    // 对称转移 (symmetric transfer): 返回子协程句柄交给协程机制去恢复,
    // 而不是在 await_suspend 里调用 resume() —— 后者会让父协程在子协程的
    // 栈帧里继续执行, 父协程一旦跑完并销毁自己的帧, 返回时就悬垂了。
    bool await_ready() noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) {
        if (!handle_) return std::noop_coroutine();
        handle_.promise().continuation_ = h;
        return handle_;
    }
    T await_resume() { return result(); }

private:
    handle_type handle_;
};

// void 特化
template <>
class Task<void> {
public:
    struct promise_type {
        std::exception_ptr exception_;
        std::coroutine_handle<> continuation_ = nullptr;
        bool detached_ = false;

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }

        struct FinalAwaiter {
            bool await_ready() noexcept { return false; }
            std::coroutine_handle<> await_suspend(
                std::coroutine_handle<promise_type> h) noexcept
            {
                auto& p = h.promise();
                if (p.continuation_) {
                    auto cont = p.continuation_;
                    p.continuation_ = nullptr;
                    return cont;
                }
                if (p.detached_) {
                    h.destroy();
                }
                return std::noop_coroutine();
            }
            void await_resume() noexcept {}
        };
        FinalAwaiter final_suspend() noexcept { return {}; }

        void return_void() {}
        void unhandled_exception() { exception_ = std::current_exception(); }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    explicit Task(handle_type h) : handle_(h) {}
    ~Task() { if (handle_) handle_.destroy(); }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&& other) noexcept : handle_(other.handle_) { other.handle_ = nullptr; }

    void result() const {
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
    }

    auto handle() { return handle_; }

    // 启动并放弃所有权 (fire-and-forget), 帧在 final_suspend 处自毁
    void detach() {
        auto h = handle_;
        handle_ = nullptr;
        if (h) {
            h.promise().detached_ = true;
            h.resume();
        }
    }

    bool await_ready() noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) {
        if (!handle_) return std::noop_coroutine();
        handle_.promise().continuation_ = h;
        return handle_;   // 对称转移, 理由同 Task<T>
    }
    void await_resume() { result(); }

private:
    handle_type handle_;
};

// ============================================================================
// AsyncRecvAwaiter - co_await 的 awaitable (异步接收)
// ============================================================================
// ctx_ 是 awaiter 的成员 → 存放在协程帧上, 生命周期覆盖整个挂起期,
// 引擎在完成前一直持有它的指针 (因此不要 co_await 一个长生命周期的 awaiter
// 对象, 用临时对象即可: 临时量活到 co_await 全表达式结束, 覆盖挂起期)。
class AsyncRecvAwaiter {
public:
    AsyncRecvAwaiter(IOEngineUring* engine, socket_t fd, uint8_t* buffer, size_t len)
        : engine_(engine), fd_(fd), buffer_(buffer), len_(len)
    {
        ctx_.user_data = this;                            // 完成事件找回 awaiter
        ctx_.on_complete = &AsyncRecvAwaiter::on_cqe;      // 引擎收割后回调
    }

    bool await_ready() noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) {
        continuation_ = h;

        ctx_.op = IOOperation::RECV;
        ctx_.fd = fd_;
        ctx_.buffer = buffer_;
        ctx_.buffer_len = len_;
        ctx_.bytes_transferred = 0;
        ctx_.error = 0;

        if (engine_->post_recv(fd_, &ctx_) != 0) {
            // 投递失败 (SQ 满等): 不能挂起, 否则永远等不到完成事件。
            // h.resume() 必须是最后一条语句 —— 恢复后协程可能直接跑完并
            // 销毁自己的帧, 那样 this (awaiter 在帧上) 也就没了。
            ctx_.bytes_transferred = 0;
            ctx_.error = ECANCELED;
            h.resume();
        }
    }

    size_t await_resume() noexcept {
        if (ctx_.error != 0) {
            return 0;  // 错误 → 返回 0 (与 EOF 同一处理路径)
        }
        return ctx_.bytes_transferred;
    }

    // 由引擎在 CQE 收割后调用: 找回 awaiter 并恢复协程。
    // 注意: resume() 返回后协程帧可能已被销毁 (detach 的任务), 不能再访问成员。
    static void on_cqe(IOContext* base) {
        auto* self = static_cast<AsyncRecvAwaiter*>(base->user_data);
        if (self->continuation_) {
            self->continuation_.resume();
        }
    }

    UringContext ctx_;

private:
    IOEngineUring* engine_;
    socket_t fd_;
    uint8_t* buffer_;
    size_t len_;
    std::coroutine_handle<> continuation_;
};

// ============================================================================
// AsyncSendAwaiter - co_await 的 awaitable (异步发送)
// ============================================================================
class AsyncSendAwaiter {
public:
    AsyncSendAwaiter(IOEngineUring* engine, socket_t fd, const uint8_t* data, size_t len)
        : engine_(engine), fd_(fd), data_(data), len_(len)
    {
        ctx_.user_data = this;
        ctx_.on_complete = &AsyncSendAwaiter::on_cqe;
    }

    bool await_ready() noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) {
        continuation_ = h;

        ctx_.op = IOOperation::SEND;
        ctx_.fd = fd_;
        ctx_.buffer = const_cast<uint8_t*>(data_);
        ctx_.bytes_transferred = len_;   // post_send 以此作为待发送长度
        ctx_.error = 0;

        if (engine_->post_send(fd_, &ctx_) != 0) {
            ctx_.bytes_transferred = 0;
            ctx_.error = ECANCELED;
            h.resume();   // 必须是最后一条语句, 理由同 recv
        }
    }

    size_t await_resume() noexcept {
        if (ctx_.error != 0) return 0;
        return ctx_.bytes_transferred;
    }

    static void on_cqe(IOContext* base) {
        auto* self = static_cast<AsyncSendAwaiter*>(base->user_data);
        if (self->continuation_) {
            self->continuation_.resume();
        }
    }

    UringContext ctx_;

private:
    IOEngineUring* engine_;
    socket_t fd_;
    const uint8_t* data_;
    size_t len_;
    std::coroutine_handle<> continuation_;
};

// ============================================================================
// 便捷工厂函数
// ============================================================================
inline AsyncRecvAwaiter async_recv(IOEngineUring* engine, socket_t fd,
                                     uint8_t* buffer, size_t len) {
    return AsyncRecvAwaiter(engine, fd, buffer, len);
}

inline AsyncSendAwaiter async_send(IOEngineUring* engine, socket_t fd,
                                     const uint8_t* data, size_t len) {
    return AsyncSendAwaiter(engine, fd, data, len);
}

} // namespace coro
} // namespace gateway
} // namespace cyrus
