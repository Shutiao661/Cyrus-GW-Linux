// ============================================================================
// server.cpp - 网关服务器实现
// ============================================================================

#include "cyrus/gateway/server.hpp"
#include "cyrus/gateway/router.hpp"
#include "cyrus/logger.hpp"

namespace cyrus {
namespace gateway {

// ============================================================================
// 构造/析构
// ============================================================================
Server::Server(const Config& config) : config_(config) {
    // 读取配置
    listen_address_ = config_.get("server", "listen_address", "0.0.0.0");
    listen_port_    = static_cast<uint16_t>(config_.get_int("server", "listen_port", 8080));
    worker_count_   = config_.get_int("server", "worker_threads", 0);

    // 如果没有配置工作线程数, 使用 CPU 核心数
    if (worker_count_ <= 0) {
        worker_count_ = static_cast<int>(std::thread::hardware_concurrency());
        if (worker_count_ <= 0) worker_count_ = 4;  // fallback
    }

    LOG_INFO("Server config: {}:{}, {} workers",
             listen_address_, listen_port_, worker_count_);
}

Server::~Server() {
    stop();
}

// ============================================================================
// start() - 启动服务器
// ============================================================================
bool Server::start() {
    // --- 第 1 步: 创建 I/O 引擎 ---
    engine_ = create_io_engine();
    if (!engine_) {
        LOG_ERROR("Failed to create I/O engine");
        return false;
    }
    if (!engine_->init()) {
        LOG_ERROR("Failed to initialize I/O engine");
        return false;
    }

    // --- 第 2 步: 创建缓冲池 ---
    pool_ = std::make_unique<BufferPool>(
        BufferPool::DEFAULT_BUFFER_COUNT,
        BufferPool::DEFAULT_BUFFER_SIZE);

    // --- 第 3 步: 创建限流器 ---
    int global_rate     = config_.get_int("rate_limit", "global_rate", 1000);
    int global_capacity = config_.get_int("rate_limit", "global_capacity", 2000);
    int per_ip_rate     = config_.get_int("rate_limit", "per_ip_rate", 50);
    int per_ip_capacity = config_.get_int("rate_limit", "per_ip_capacity", 100);
    rate_limiter_ = std::make_unique<RateLimiter>(
        global_rate, global_capacity, per_ip_rate, per_ip_capacity);
    LOG_INFO("Rate limiter: global={}/s burst={}, per_ip={}/s burst={}",
             global_rate, global_capacity, per_ip_rate, per_ip_capacity);

    // --- 第 4 步: 创建并初始化路由器 ---
    router_ = std::make_unique<Router>(config_);
    if (!router_->init()) {
        LOG_WARN("Router: agent backend may not be available (non-fatal)");
    }

    // --- 第 5 步: 创建监听 socket ---
    if (!create_listen_socket()) {
        return false;
    }

    // --- 第 6 步: 注册监听 socket 到 IOCP ---
    // 注意: 监听 socket 的 user_data 设为 nullptr (accept completion 中判断)
    engine_->register_socket(listen_fd_, nullptr);

    // --- 第 5 步: 投递初始 Accept 操作 ---
    int initial_accepts = worker_count_ * 2;  // 每个 worker 投递 2 个 accept
    for (int i = 0; i < initial_accepts; ++i) {
        if (!post_accept()) {
            LOG_ERROR("Failed to post initial accept #{}", i);
            return false;
        }
    }
    LOG_INFO("Posted {} initial accept operations", initial_accepts);

    // --- 第 6 步: 启动工作线程 ---
    g_running.store(true);
    worker_threads_.reserve(worker_count_);
    for (int i = 0; i < worker_count_; ++i) {
        worker_threads_.emplace_back(&Server::worker_loop, this, i);
    }
    LOG_INFO("Started {} worker threads", worker_count_);

    started_ = true;
    LOG_INFO("🚀 Cyrus-GW Gateway listening on {}:{}",
             listen_address_, listen_port_);
    return true;
}

// ============================================================================
// wait_for_shutdown() - 等待关闭信号
// ============================================================================
void Server::wait_for_shutdown() {
    if (!started_) return;

    // 主线程以 500ms 间隔检查 g_running 标志
    // 信号处理器 (Ctrl+C) 会将 g_running 设置为 false
    while (g_running.load(std::memory_order_acquire)) {
        cyrus_sleep_ms(500);
    }

    LOG_INFO("Shutdown signal received");
    stop();
}

// ============================================================================
// stop() - 关闭服务器
// ============================================================================
void Server::stop() {
    if (!started_) return;

    LOG_INFO("Shutting down server...");
    g_running.store(false, std::memory_order_release);

    // --- 第 1 步: 关闭监听 socket (停止接受新连接) ---
    if (listen_fd_ != INVALID_SOCKET_VAL) {
        cyrus_close_socket(listen_fd_);
        listen_fd_ = INVALID_SOCKET_VAL;
    }

    // --- 第 2 步: 唤醒工作线程 ---
    engine_->post_wakeup();

    // --- 第 3 步: 等待工作线程退出 ---
    for (auto& t : worker_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    worker_threads_.clear();
    LOG_INFO("All worker threads stopped");

    // --- 第 4 步: 关闭所有连接 ---
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        for (auto& [fd, conn] : connections_) {
            conn->close();
        }
        connections_.clear();
    }
    LOG_INFO("All connections closed");

    // --- 第 5 步: 关闭引擎 ---
    engine_->shutdown();
    engine_.reset();

    started_ = false;
    LOG_INFO("Server stopped");
}

// ============================================================================
// create_listen_socket() - 创建监听 socket
// ============================================================================
bool Server::create_listen_socket() {
    // 创建 TCP socket
    listen_fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listen_fd_ == INVALID_SOCKET_VAL) {
        LOG_ERROR("Failed to create listen socket: {}", cyrus_socket_error());
        return false;
    }

    // 设置 SO_REUSEADDR (允许快速重启, 即使端口处于 TIME_WAIT)
    int reuse = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR,
                   reinterpret_cast<const char*>(&reuse), sizeof(reuse)) == SOCKET_ERROR_VAL) {
        LOG_WARN("setsockopt(SO_REUSEADDR) failed: {}", cyrus_socket_error());
        // 非致命错误, 继续
    }

    // 设置非阻塞 (IOCP 需要)
    cyrus_set_nonblocking(listen_fd_);

    // 绑定地址
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(listen_port_);
    if (listen_address_ == "0.0.0.0" || listen_address_ == "*") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        inet_pton(AF_INET, listen_address_.c_str(), &addr.sin_addr);
    }

    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR_VAL) {
        LOG_ERROR("Failed to bind to {}:{}: {}",
                  listen_address_, listen_port_, cyrus_socket_error());
        cyrus_close_socket(listen_fd_);
        listen_fd_ = INVALID_SOCKET_VAL;
        return false;
    }

    // 开始监听 (backlog = SOMAXCONN: 使用系统最大等待队列)
    if (listen(listen_fd_, SOMAXCONN) == SOCKET_ERROR_VAL) {
        LOG_ERROR("Failed to listen: {}", cyrus_socket_error());
        cyrus_close_socket(listen_fd_);
        listen_fd_ = INVALID_SOCKET_VAL;
        return false;
    }

    LOG_INFO("Listen socket created: fd={}", static_cast<int>(listen_fd_));
    return true;
}

// ============================================================================
// post_accept() - 投递异步 Accept
// ============================================================================
bool Server::post_accept() {
    IOContext* ctx = engine_->acquire_context();
    ctx->user_data = nullptr;  // 标记为 Accept 操作 (worker_loop 用于区分)

    int result = engine_->post_accept(listen_fd_, ctx);
    if (result != 0) {
        LOG_ERROR("Failed to post accept: err={}", cyrus_socket_error());
        engine_->release_context(ctx);
        return false;
    }
    return true;
}

// ============================================================================
// worker_loop() - 工作线程事件循环
// ============================================================================
void Server::worker_loop(int worker_id) {
    LOG_DEBUG("Worker {} started", worker_id);

    constexpr int MAX_COMPLETIONS = 64;  // 单次获取的最大完成事件数
    IOContext* completions[MAX_COMPLETIONS];

    while (g_running.load(std::memory_order_acquire)) {
        // --- 等待完成事件 (1 秒超时, 允许定期检查 g_running) ---
        int n = engine_->wait_completions(completions, MAX_COMPLETIONS, 1000);

        for (int i = 0; i < n; ++i) {
            IOContext* ctx = completions[i];

            // 跳过空事件 (wakeup 事件)
            if (ctx == nullptr) continue;

            // --- 按操作类型分发 ---
            switch (ctx->op) {
                case IOOperation::ACCEPT:
                    on_accept_complete(ctx);
                    break;

                case IOOperation::RECV:
                case IOOperation::SEND: {
                    // user_data 指向 Connection 对象
                    Connection* conn = static_cast<Connection*>(ctx->user_data);
                    if (conn) {
                        if (ctx->op == IOOperation::RECV) {
                            conn->on_recv_complete(ctx);
                        } else {
                            conn->on_send_complete(ctx);
                        }
                    }
                    break;
                }

                default:
                    LOG_WARN("Unknown IO operation type: {}",
                             static_cast<int>(ctx->op));
                    engine_->release_context(ctx);
                    break;
            }
        }
    }

    LOG_DEBUG("Worker {} exiting", worker_id);
}

// ============================================================================
// on_accept_complete() - Accept 完成处理
// ============================================================================
void Server::on_accept_complete(IOContext* ctx) {
    if (ctx->error != 0 || ctx->accept_fd == INVALID_SOCKET_VAL) {
        LOG_WARN("Accept failed: err={}", ctx->error);
        // 归还上下文, 重新投递 accept
        engine_->release_context(ctx);
        if (g_running.load(std::memory_order_acquire)) {
            post_accept();
        }
        return;
    }

    socket_t client_fd = ctx->accept_fd;

    // --- 第 1 步: 设置非阻塞 ---
    cyrus_set_nonblocking(client_fd);

#ifdef _WIN32
    // SO_UPDATE_ACCEPT_CONTEXT: Windows 必须! 让新 socket 继承监听 socket 属性
    setsockopt(client_fd, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
               reinterpret_cast<const char*>(&listen_fd_), sizeof(listen_fd_));
#endif

    // --- 第 2 步: 禁用 Nagle 算法 (低延迟响应) ---
    int nodelay = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY,
               reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));

    // --- 第 3 步: 创建 Connection 对象 ---
    auto conn = std::make_unique<Connection>(
        client_fd, engine_.get(), pool_.get(),
        router_.get(), rate_limiter_.get());

    // --- 第 4 步: 启动连接 (注册到引擎 + 投递第一个 recv) ---
    Connection* conn_ptr = conn.get();
    add_connection(client_fd, std::move(conn));
    conn_ptr->on_accept_complete();

    // --- 第 5 步: 归还 Accept 上下文 ---
    engine_->release_context(ctx);

    // --- 第 6 步: 重新投递 Accept (保持多个 accept 在队列中) ---
    post_accept();
}

// ============================================================================
// 连接管理
// ============================================================================
void Server::add_connection(socket_t fd, std::unique_ptr<Connection> conn) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_[fd] = std::move(conn);
}

void Server::remove_connection(socket_t fd) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_.erase(fd);
}

} // namespace gateway
} // namespace cyrus
