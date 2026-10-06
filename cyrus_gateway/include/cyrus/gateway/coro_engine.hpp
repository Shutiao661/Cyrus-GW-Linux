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
// 工作原理:
//   1. async_recv(fd, buffer) 返回一个 Awaitable 对象
//   2. co_await 挂起协程, 投递 io_uring recv 操作
//   3. io_uring CQE 到达时恢复协程
//   4. 协程从挂起点继续执行, async_recv 返回接收到的字节数
//
// 示例:
//   Task<void> handle_client(int fd, BufferPool* pool) {
//       auto buf = pool->acquire();
//       size_t n = co_await async_recv(fd, buf.data(), buf.capacity());
//       if (n > 0) {
//           co_await async_send(fd, buf.data(), n);
//       }
//   }
//
// 平台限制:
//   - Linux + GCC 10+/Clang 14+: 完整支持
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
// Task<T> - 协程返回值 (惰性求值, 仅在 co_await 时启动)
// ============================================================================
template <typename T = void>
class Task {
public:
    struct promise_type {
        T result_;
        std::exception_ptr exception_;
        std::coroutine_handle<> continuation_;  // 等待此 Task 的协程

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }

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

    // Awaitable: 允许 co_await task
    bool await_ready() noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
        handle_.promise().continuation_ = h;
        handle_.resume();  // 启动被等待的协程
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
        std::coroutine_handle<> continuation_;

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
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

    bool await_ready() noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
        handle_.promise().continuation_ = h;
        handle_.resume();
    }
    void await_resume() { result(); }

private:
    handle_type handle_;
};

// ============================================================================
// AsyncRecvAwaiter - co_await 的 awaitable (异步接收)
// ============================================================================
class AsyncRecvAwaiter {
public:
    AsyncRecvAwaiter(IOEngineUring* engine, socket_t fd, uint8_t* buffer, size_t len)
        : engine_(engine), fd_(fd), buffer_(buffer), len_(len) {}

    bool await_ready() noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) {
        continuation_ = h;

        // 构造上下文
        ctx_.op = IOOperation::RECV;
        ctx_.fd = fd_;
        ctx_.buffer = buffer_;
        ctx_.buffer_len = len_;
        ctx_.user_data = this;

        // 投递异步 recv
        engine_->post_recv(fd_, &ctx_);
    }

    size_t await_resume() noexcept {
        if (ctx_.error != 0) {
            return 0;  // 错误 → 返回 0
        }
        return ctx_.bytes_transferred;
    }

    // 完成时由 io_uring 回调调用
    void on_complete() {
        if (continuation_) {
            continuation_.resume();
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
        : engine_(engine), fd_(fd), data_(data), len_(len) {}

    bool await_ready() noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) {
        continuation_ = h;

        ctx_.op = IOOperation::SEND;
        ctx_.fd = fd_;
        ctx_.buffer = const_cast<uint8_t*>(data_);
        ctx_.bytes_transferred = len_;
        ctx_.user_data = this;

        engine_->post_send(fd_, &ctx_);
    }

    size_t await_resume() noexcept {
        if (ctx_.error != 0) return 0;
        return ctx_.bytes_transferred;
    }

    void on_complete() {
        if (continuation_) continuation_.resume();
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
