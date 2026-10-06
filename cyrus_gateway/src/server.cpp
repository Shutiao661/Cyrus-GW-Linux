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

    max_connections_ = static_cast<size_t>(
        config_.get_int("connection", "max_connections", 10000));
    keepalive_timeout_ms_ = static_cast<int64_t>(
        config_.get_int("connection", "keepalive_timeout_s", 5)) * 1000;
    max_keepalive_requests_ = static_cast<size_t>(
        config_.get_int("connection", "max_keepalive_requests", 1000));
    max_header_size_ = static_cast<size_t>(
        config_.get_int("connection", "max_header_size", 8192));

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
    // 流式连接在中继结束后由中继线程摘除 (它是这条连接的最后使用者)
    router_->set_connection_reaper([this](socket_t fd) { remove_connection(fd); });

    // --- 第 5 步: 创建监听 socket ---
    if (!create_listen_socket()) {
        return false;
    }

    // --- 第 6 步: 注册监听 socket ---
    // 注意: 监听 socket 的 user_data 设为 nullptr (accept completion 中判断)
    engine_->register_socket(listen_fd_, nullptr);

    // --- 第 7 步: 投递初始 Accept 操作 ---
    int initial_accepts = worker_count_ * 2;  // 每个 worker 投递 2 个 accept
    for (int i = 0; i < initial_accepts; ++i) {
        if (!post_accept()) {
            LOG_ERROR("Failed to post initial accept #{}", i);
            return false;
        }
    }
    LOG_INFO("Posted {} initial accept operations", initial_accepts);

    // --- 第 8 步: 启动工作线程 ---
    g_running.store(true);
    worker_threads_.reserve(worker_count_);
    for (int i = 0; i < worker_count_; ++i) {
        worker_threads_.emplace_back(&Server::worker_loop, this, i);
    }
    LOG_INFO("Started {} worker threads", worker_count_);

    // --- 第 9 步: 启动空闲连接回收线程 ---
    sweeper_thread_ = std::thread(&Server::sweeper_loop, this);

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

    // 主线程安全地执行信号回调 (信号处理器本身只做了原子操作)
    drain_signal_callback();
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

    // --- 第 1 步: 停止接受新连接 ---
    // 注意: 不在此处关闭监听 socket。POSIX fallback 下 accept 线程阻塞在
    // accept() 上, close(fd) 无法解除其阻塞; 引擎的 shutdown() 会用
    // shutdown(SHUT_RDWR) 正确关闭监听 socket 并 join accept 线程。
    listen_fd_ = INVALID_SOCKET_VAL;

    // --- 第 2 步: 排空 SSE 中继线程 ---
    // 中继线程持有 Connection*/AgentClient* 裸指针, 必须在销毁两者之前 join,
    // 否则 use-after-free
    if (router_) {
        router_->drain_relays();
    }

    // --- 第 3 步: 关闭所有客户端 socket (仅关 fd, 对象暂不销毁) ---
    // 关键: POSIX fallback 下 worker 线程阻塞在同步 ::recv 上, 若不先关闭 socket,
    // join 会永久阻塞 (死锁)。Connection::close() 用 shutdown(SHUT_RDWR)
    // 让阻塞中的 ::recv 立即返回 (仅 SHUT_WR 或 close(fd) 都唤不醒)。
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        for (auto& [fd, conn] : connections_) {
            conn->close();
        }
    }

    // --- 第 4 步: 唤醒工作线程 (解除 wait_completions 的阻塞) ---
    engine_->post_wakeup();

    // --- 第 5 步: 等待工作线程退出 ---
    for (auto& t : worker_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    worker_threads_.clear();

    // --- 第 5.5 步: 停止空闲连接回收线程 (必须在销毁连接对象之前) ---
    if (sweeper_thread_.joinable()) {
        sweeper_thread_.join();
    }
    LOG_INFO("All worker threads stopped");

    // --- 第 6 步: 销毁连接对象 (worker 已全部退出, 无 use-after-free) ---
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        connections_.clear();
    }
    LOG_INFO("All connections closed");

    // --- 第 7 步: 关闭引擎 ---
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

    // 设置非阻塞 (io_uring 需要)
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
    // 关闭流程中 listen_fd_ 已被置为无效: 不能再投递 accept。
    // 若把无效 fd 传下去, 引擎侧的 listen_fd_ 会被覆盖成 -1, 导致
    // shutdown() 跳过"关闭监听 socket"分支, accept 线程永远卡在 accept()
    // 上无法 join (关机永久挂起)。
    if (listen_fd_ == INVALID_SOCKET_VAL) {
        return false;
    }

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
                        // 每个连接同一时刻只有一个挂起的操作, 因此本线程
                        // 处理完这个完成事件后即成为该连接的最后使用者
                        socket_t conn_fd = ctx->fd;
                        if (ctx->op == IOOperation::RECV) {
                            conn->on_recv_complete(ctx);
                        } else {
                            conn->on_send_complete(ctx);
                        }
                        // 已关闭且无中继线程持有 → 摘除并销毁 (修复连接对象泄漏)。
                        // remove_connection 之后不得再访问 conn。
                        if (conn->is_closed() && !conn->is_streaming()) {
                            remove_connection(conn_fd);
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

    // --- 第 1 步: 连接数上限 (防雪崩: 超限直接拒绝, 不建立 Connection) ---
    if (at_connection_limit()) {
        LOG_WARN("Max connections reached ({}), rejecting fd={}",
                 max_connections_, static_cast<int>(client_fd));
        cyrus_close_socket(client_fd);
        engine_->release_context(ctx);
        if (g_running.load(std::memory_order_acquire)) {
            post_accept();
        }
        return;
    }

    // --- 第 2 步: 设置非阻塞 + 禁用 Nagle (低延迟响应) ---
    cyrus_set_nonblocking(client_fd);

    int nodelay = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY,
               reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));

    // --- 第 3 步: 提取客户端 IP (per-IP 限流用) ---
    std::string client_ip;
    char ipbuf[INET_ADDRSTRLEN] = {0};
    if (inet_ntop(AF_INET, &ctx->accept_addr.sin_addr, ipbuf, sizeof(ipbuf)) != nullptr) {
        client_ip = ipbuf;
    }

    // --- 第 4 步: 创建 Connection 对象 (带上配置的连接级上限) ---
    auto conn = std::make_unique<Connection>(
        client_fd, engine_.get(), pool_.get(),
        router_.get(), rate_limiter_.get(), client_ip,
        max_keepalive_requests_, max_header_size_);

    // --- 第 5 步: 启动连接 (注册到引擎 + 投递第一个 recv) ---
    Connection* conn_ptr = conn.get();
    add_connection(client_fd, std::move(conn));
    conn_ptr->on_accept_complete();

    // --- 第 6 步: 归还 Accept 上下文 ---
    engine_->release_context(ctx);

    // --- 第 7 步: 重新投递 Accept (保持多个 accept 在队列中) ---
    post_accept();
}

// ============================================================================
// 连接管理
// ============================================================================
void Server::add_connection(socket_t fd, std::unique_ptr<Connection> conn) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_[fd] = std::move(conn);
}

// 摘除并销毁连接。只允许"最后使用者"调用:
//   - 处理完该连接完成事件的 worker 线程
//   - 持有该连接的 SSE 中继线程 (排空时保证已 join)
void Server::remove_connection(socket_t fd) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    size_t erased = connections_.erase(fd);
    LOG_DEBUG("Connection removed: fd={}, active={}", static_cast<int>(fd),
              connections_.size());
    (void)erased;
}

// 是否已达连接数上限。加锁读取, 与 add_connection/remove_connection 保持一致。
bool Server::at_connection_limit() {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    return connections_.size() >= max_connections_;
}

// ============================================================================
// sweeper_loop() - 空闲连接回收线程
// ============================================================================
void Server::sweeper_loop() {
    while (g_running.load(std::memory_order_acquire)) {
        cyrus_sleep_ms(250);
        if (!g_running.load(std::memory_order_acquire)) break;
        sweep_idle_connections();
    }
}

// ============================================================================
// sweep_idle_connections() - 回收空闲超时的 keep-alive 连接
// ============================================================================
// 在锁内挑出候选 (只读), 在锁外关闭 —— 避免在持锁期间调用 close()。
// 正在被 SSE 中继持有的连接跳过: 长流可能长时间没有数据, 但它并不是空闲。
void Server::sweep_idle_connections() {
    std::vector<Connection*> expired;
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        for (auto& [fd, conn] : connections_) {
            if (conn->is_closed() || conn->is_streaming()) continue;
            if (conn->idle_ms() > keepalive_timeout_ms_) {
                expired.push_back(conn.get());
            }
        }
    }

    for (Connection* conn : expired) {
        socket_t fd = conn->fd();
        LOG_DEBUG("Keep-alive timeout: fd={}, idle={}ms",
                  static_cast<int>(fd), conn->idle_ms());
        // 只关闭, 不在此处销毁: 该连接可能仍有挂起的 recv,
        // 销毁由处理那个完成事件的 worker 线程完成 (它会看到 is_closed())。
        conn->on_timeout();
    }
}

} // namespace gateway
} // namespace cyrus
