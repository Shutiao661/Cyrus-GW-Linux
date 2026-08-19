// ============================================================================
// io_engine_uring.cpp - Linux io_uring I/O 引擎实现
// ============================================================================
// 两种实现路径:
//   CYRUS_HAS_LIBURING=1 → 完整 io_uring 实现
//   CYRUS_HAS_LIBURING=0 → POSIX fallback (accept线程 + 同步recv/send)
// ============================================================================

#include "cyrus/gateway/io_engine_uring.hpp"

#if CYRUS_HAS_LIBURING
#include <liburing.h>
#endif
#include <unistd.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <deque>
#include <condition_variable>

namespace cyrus {
namespace gateway {

// ============================================================================
// 析构
// ============================================================================
IOEngineUring::~IOEngineUring() {
    shutdown();
}

// ============================================================================
// init() - 初始化引擎
// ============================================================================
bool IOEngineUring::init() {
#if CYRUS_HAS_LIBURING
    constexpr int SQ_ENTRIES = 256;
    int ret = io_uring_queue_init(SQ_ENTRIES, &ring_, 0);
    if (ret < 0) {
        LOG_ERROR("io_uring_queue_init failed: {} ({})", -ret, strerror(-ret));
        return false;
    }
    initialized_ = true;
    LOG_INFO("IOEngineUring initialized ({} SQ entries)", SQ_ENTRIES);
    return true;
#else
    initialized_ = true;
    LOG_INFO("IOEngineUring initialized (POSIX fallback mode)");
    return true;
#endif
}

// ============================================================================
// shutdown() - 关闭引擎
// ============================================================================
void IOEngineUring::shutdown() {
    if (!initialized_) return;
    shutting_down_.store(true, std::memory_order_release);

#if CYRUS_HAS_LIBURING
    for (int i = 0; i < 64; ++i) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (sqe) { io_uring_prep_nop(sqe); sqe->user_data = 0; io_uring_submit(&ring_); }
    }
    io_uring_queue_exit(&ring_);
#else
    // 唤醒 accept 线程和 worker 线程
    post_wakeup();
    if (accept_thread_.joinable()) {
        // 关闭监听 socket 使 accept() 返回
        if (listen_fd_ != INVALID_SOCKET_VAL) ::close(listen_fd_);
        accept_thread_.join();
    }
#endif
    initialized_ = false;
    LOG_INFO("IOEngineUring shutdown complete");
}

// ============================================================================
// register_socket() - 注册 fd
// ============================================================================
bool IOEngineUring::register_socket(socket_t fd, void* user_data) {
    (void)fd;
    (void)user_data;
#if CYRUS_HAS_LIBURING
    return true;
#else
    // POSIX fallback: 记住监听 fd 用于 accept 线程
    if (listen_fd_ == INVALID_SOCKET_VAL) listen_fd_ = fd;
    return true;
#endif
}

// ============================================================================
// post_accept() - 投递 Accept
// ============================================================================
int IOEngineUring::post_accept(socket_t listen_fd, IOContext* base_ctx) {
#if CYRUS_HAS_LIBURING
    UringContext* ctx = static_cast<UringContext*>(base_ctx);
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) { LOG_ERROR("SQ full"); return -1; }
    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);
    io_uring_prep_accept(sqe, static_cast<int>(listen_fd),
                         reinterpret_cast<sockaddr*>(&client_addr), &addr_len, 0);
    ctx->op = IOOperation::ACCEPT;
    ctx->fd = listen_fd;
    sqe->user_data = UringContext::encode(IOOperation::ACCEPT, ctx);
    io_uring_submit(&ring_);
    return 0;
#else
    // POSIX fallback: 启动 accept 线程 (首次调用时)
    listen_fd_ = listen_fd;
    base_ctx->op = IOOperation::ACCEPT;
    base_ctx->fd = listen_fd;

    if (!accept_thread_running_) {
        accept_thread_running_ = true;
        accept_thread_ = std::thread([this]() {
            while (!shutting_down_.load(std::memory_order_acquire)) {
                sockaddr_in client_addr{};
                socklen_t addr_len = sizeof(client_addr);
                socket_t client_fd = ::accept(listen_fd_,
                    reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
                if (client_fd == INVALID_SOCKET_VAL) {
                    if (shutting_down_.load()) break;
                    continue;
                }
                // 从池中获取 context 并填充结果
                IOContext* ctx = acquire_context();
                ctx->op = IOOperation::ACCEPT;
                ctx->fd = listen_fd_;
                ctx->accept_fd = client_fd;
                ctx->error = 0;

                // 推入完成队列
                {
                    std::lock_guard<std::mutex> lk(completion_mutex_);
                    completion_queue_.push_back(ctx);
                }
                completion_cv_.notify_one();

                // 重新投递: 确保队列中有足够的 accept context
                // (由 Server 的 on_accept_complete 中 post_accept() 处理)
            }
        });
    }
    return 0;
#endif
}

// ============================================================================
// post_recv() - 投递 Recv
// ============================================================================
int IOEngineUring::post_recv(socket_t fd, IOContext* base_ctx) {
#if CYRUS_HAS_LIBURING
    UringContext* ctx = static_cast<UringContext*>(base_ctx);
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) { LOG_ERROR("SQ full"); return -1; }
    ctx->op = IOOperation::RECV; ctx->fd = fd;
    ctx->bytes_transferred = 0; ctx->error = 0;
    io_uring_prep_recv(sqe, static_cast<int>(fd), ctx->buffer, ctx->buffer_len, 0);
    sqe->user_data = UringContext::encode(IOOperation::RECV, ctx);
    io_uring_submit(&ring_);
    return 0;
#else
    // POSIX fallback: 同步 recv (worker 线程中阻塞)
    base_ctx->op = IOOperation::RECV;
    base_ctx->fd = fd;
    int n = ::recv(static_cast<int>(fd), base_ctx->buffer, base_ctx->buffer_len, 0);
    if (n > 0) {
        base_ctx->bytes_transferred = static_cast<size_t>(n);
        base_ctx->error = 0;
    } else if (n == 0) {
        base_ctx->bytes_transferred = 0; base_ctx->error = 0;  // EOF
    } else {
        base_ctx->bytes_transferred = 0;
        base_ctx->error = (errno == EAGAIN || errno == EWOULDBLOCK) ? 0 : errno;
    }
    // 推入完成队列
    {
        std::lock_guard<std::mutex> lk(completion_mutex_);
        completion_queue_.push_back(base_ctx);
    }
    completion_cv_.notify_one();
    return 0;
#endif
}

// ============================================================================
// post_send() - 投递 Send
// ============================================================================
int IOEngineUring::post_send(socket_t fd, IOContext* base_ctx) {
#if CYRUS_HAS_LIBURING
    UringContext* ctx = static_cast<UringContext*>(base_ctx);
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) { LOG_ERROR("SQ full"); return -1; }
    ctx->op = IOOperation::SEND; ctx->fd = fd;
    io_uring_prep_send(sqe, static_cast<int>(fd), ctx->buffer, ctx->bytes_transferred, 0);
    sqe->user_data = UringContext::encode(IOOperation::SEND, ctx);
    io_uring_submit(&ring_);
    return 0;
#else
    // POSIX fallback: 同步 send
    base_ctx->op = IOOperation::SEND;
    base_ctx->fd = fd;
    int n = ::send(static_cast<int>(fd), base_ctx->buffer,
                   base_ctx->bytes_transferred, 0);
    if (n >= 0) {
        base_ctx->bytes_transferred = static_cast<size_t>(n);
        base_ctx->error = 0;
    } else {
        base_ctx->bytes_transferred = 0;
        base_ctx->error = errno;
    }
    {
        std::lock_guard<std::mutex> lk(completion_mutex_);
        completion_queue_.push_back(base_ctx);
    }
    completion_cv_.notify_one();
    return 0;
#endif
}

// ============================================================================
// wait_completions() - 等待完成事件
// ============================================================================
int IOEngineUring::wait_completions(IOContext** contexts, int max_events,
                                     int timeout_ms) {
#if CYRUS_HAS_LIBURING
    if (shutting_down_.load(std::memory_order_acquire)) return 0;
    struct io_uring_cqe* cqes[128];
    int to_get = (max_events < 128) ? max_events : 128;
    int ret;
    if (timeout_ms < 0) {
        ret = io_uring_wait_cqe(&ring_, &cqes[0]);
        if (ret == 0) to_get = 1; else return 0;
    } else if (timeout_ms == 0) {
        ret = io_uring_peek_batch_cqe(&ring_, cqes, static_cast<unsigned>(to_get));
        if (ret < 0) return 0;
        to_get = ret;
    } else {
        ret = io_uring_peek_batch_cqe(&ring_, cqes, static_cast<unsigned>(to_get));
        if (ret > 0) { to_get = ret; }
        else {
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
            if (sqe) {
                struct __kernel_timespec ts;
                ts.tv_sec = timeout_ms / 1000;
                ts.tv_nsec = (timeout_ms % 1000) * 1000000;
                io_uring_prep_timeout(sqe, &ts, 0, 0);
                sqe->user_data = 0; io_uring_submit(&ring_);
            }
            ret = io_uring_wait_cqe(&ring_, &cqes[0]);
            if (ret < 0) return 0;
            if (cqes[0]->user_data == 0) { io_uring_cqe_seen(&ring_, cqes[0]); return 0; }
            to_get = 1;
        }
    }
    int count = 0;
    for (int i = 0; i < to_get && i < 128; ++i) {
        struct io_uring_cqe* cqe = cqes[i];
        if (cqe->user_data == 0) { io_uring_cqe_seen(&ring_, cqe); continue; }
        UringContext* ctx = UringContext::decode_ctx(cqe->user_data);
        if (cqe->res >= 0) { ctx->bytes_transferred = static_cast<size_t>(cqe->res); ctx->error = 0; }
        else { ctx->bytes_transferred = 0; ctx->error = -cqe->res; }
        contexts[count++] = ctx;
        io_uring_cqe_seen(&ring_, cqe);
    }
    return count;
#else
    // POSIX fallback: 从完成队列取事件
    if (shutting_down_.load(std::memory_order_acquire)) return 0;

    std::unique_lock<std::mutex> lk(completion_mutex_);
    if (completion_queue_.empty()) {
        if (timeout_ms < 0) {
            completion_cv_.wait(lk, [this]() {
                return !completion_queue_.empty() ||
                       shutting_down_.load(std::memory_order_acquire);
            });
        } else if (timeout_ms > 0) {
            completion_cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                [this]() { return !completion_queue_.empty(); });
        }
        // timeout_ms == 0: 不等待, 直接返回
    }

    if (shutting_down_.load(std::memory_order_acquire)) return 0;

    int count = 0;
    while (!completion_queue_.empty() && count < max_events) {
        contexts[count++] = completion_queue_.front();
        completion_queue_.pop_front();
    }
    return count;
#endif
}

// ============================================================================
// post_wakeup() - 唤醒阻塞线程
// ============================================================================
void IOEngineUring::post_wakeup() {
#if CYRUS_HAS_LIBURING
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe) { io_uring_prep_nop(sqe); sqe->user_data = 0; io_uring_submit(&ring_); }
#else
    completion_cv_.notify_all();
#endif
}

// ============================================================================
// Context 池管理
// ============================================================================
IOContext* IOEngineUring::acquire_context() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    if (free_contexts_.empty()) {
        auto ctx = std::make_unique<UringContext>();
        UringContext* ptr = ctx.get();
        context_pool_.push_back(std::move(ctx));
        return ptr;
    }
    UringContext* ctx = free_contexts_.back();
    free_contexts_.pop_back();
    ctx->reset();
    return ctx;
}

void IOEngineUring::release_context(IOContext* ctx) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    ctx->buffer = nullptr;
    ctx->buffer_len = 0;
    ctx->bytes_transferred = 0;
    free_contexts_.push_back(static_cast<UringContext*>(ctx));
}

} // namespace gateway
} // namespace cyrus
