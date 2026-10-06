// ============================================================================
// server.hpp - 网关服务器主控
// ============================================================================
// Server 类负责:
//   1. 初始化 I/O 引擎
//   2. 创建监听 socket, 绑定端口
//   3. 投递异步 Accept 操作
//   4. 管理工作线程池 (每个线程运行事件循环)
//   5. 协调连接对象的生命周期
//   6. 优雅关闭 (Ctrl+C → 停止接受 → 排空 → 退出)
//
// 架构: Proactor 模式 (io_uring)
//   主线程: 初始化, 投递初始 Accept, 等待关闭信号
//   工作线程: 调用 wait_completions, 分发完成事件到连接对象
// ============================================================================

#pragma once

#include "cyrus/types.hpp"
#include "cyrus/config.hpp"
#include "cyrus/buffer_pool.hpp"
#include "cyrus/token_bucket.hpp"
#include "io_engine.hpp"
#include "connection.hpp"
#include "router.hpp"

#include <vector>
#include <thread>
#include <memory>
#include <unordered_map>
#include <mutex>

namespace cyrus {
namespace gateway {

class Server {
public:
    explicit Server(const Config& config);
    ~Server();

    // 禁止拷贝
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // --- 生命周期 ---

    // 启动服务器 (创建 socket, bind, listen, 启动工作线程)
    // 返回 true 表示成功
    bool start();

    // 等待关闭信号 (阻塞直到 g_running == false)
    void wait_for_shutdown();

    // 关闭服务器 (停止接受, 等待现有连接排空)
    void stop();

private:
    // --- 内部方法 ---

    // 创建和配置监听 socket
    bool create_listen_socket();

    // 投递一个 Accept 操作到引擎
    bool post_accept();

    // Accept 完成回调
    void on_accept_complete(IOContext* ctx);

    // 工作线程主函数 (事件循环)
    void worker_loop(int worker_id);

    // --- 配置 ---
    Config config_;                  // 完整配置 (保留用于 Router/Limiter 初始化)
    std::string listen_address_;     // 监听地址 (如 "0.0.0.0")
    uint16_t    listen_port_;        // 监听端口 (如 8080)
    int         worker_count_;       // 工作线程数

    // --- I/O 基础设施 ---
    std::unique_ptr<IOEngine> engine_;       // I/O 引擎 (io_uring)
    std::unique_ptr<BufferPool> pool_;       // 缓冲池
    std::unique_ptr<Router> router_;         // 路由器 + Agent 连接池
    std::unique_ptr<RateLimiter> rate_limiter_;  // 限流器

    // --- 监听 ---
    socket_t listen_fd_ = INVALID_SOCKET_VAL;

    // --- 工作线程 ---
    std::vector<std::thread> worker_threads_;

    // --- 连接管理 ---
    std::mutex connections_mutex_;
    std::unordered_map<socket_t, std::unique_ptr<Connection>> connections_;

    // 注册连接
    void add_connection(socket_t fd, std::unique_ptr<Connection> conn);

    // --- 状态 ---
    bool started_ = false;
};

} // namespace gateway
} // namespace cyrus
