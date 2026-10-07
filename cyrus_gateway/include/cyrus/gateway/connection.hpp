// ============================================================================
// connection.hpp - 每连接状态机
// ============================================================================
// 管理单个 TCP 连接的生命周期:
//   ACCEPTED → READING_REQUEST → SENDING_RESPONSE
//                                     ↓
//                    (keep-alive) → IDLE → READING_REQUEST (循环)
//                    (close)      → CLOSING → CLOSED
//
// 连接对象负责:
//   1. 接收数据 → 喂入 HTTP 解析器
//   2. 请求解析完成 → 生成 HTTP 响应
//   3. 发送响应 → 决定 keep-alive 或关闭
//   4. 超时管理 → 空闲连接超时关闭
// ============================================================================

#pragma once

#include "cyrus/types.hpp"
#include "cyrus/buffer_pool.hpp"
#include "cyrus/token_bucket.hpp"
#include "http_parser.hpp"
#include "io_engine.hpp"

#include <memory>
#include <chrono>
#include <atomic>

namespace cyrus {
namespace gateway {

// 前向声明
class Server;
class Router;

// ============================================================================
// ConnectionState - 连接状态枚举
// ============================================================================
enum class ConnectionState : uint8_t {
    ACCEPTED,               // 初始: 连接已接受, 等待第一个 recv
    READING_REQUEST,        // 正在接收 HTTP 请求数据
    SENDING_RESPONSE,       // 正在发送 HTTP 响应
    IDLE,                   // 空闲 (keep-alive 模式, 等待下一个请求)
    CLOSING,                // 正在关闭
    CLOSED,                 // 已关闭
};

// ============================================================================
// Connection - 连接对象
// ============================================================================
class Connection : public std::enable_shared_from_this<Connection> {
    friend class Router;  // Router 需要访问 send_response/send_error
public:
    // --- 配置常量 ---
    static constexpr int    KEEPALIVE_TIMEOUT_MS   = 5000;    // keep-alive 空闲超时 (5秒)
    static constexpr int    BODY_READ_TIMEOUT_MS   = 30000;   // 请求体读取超时 (30秒)
    static constexpr size_t MAX_KEEPALIVE_REQUESTS = 1000;    // keep-alive 最大请求数

    // conn_id: 连接的唯一标识, 由 Server 分配 (单调递增)。
    // 不能用 fd 当标识: close() 之后内核会立刻复用该 fd 号, 而连接对象可能
    // 还没被摘除 —— 用 fd 做主键会让新连接覆盖(析构)仍在被使用的旧连接。
    Connection(uint64_t conn_id, socket_t fd, IOEngine* engine, BufferPool* pool,
               Router* router = nullptr, RateLimiter* rate_limiter = nullptr,
               std::string client_ip = "",
               size_t max_keepalive_requests = MAX_KEEPALIVE_REQUESTS,
               size_t max_header_size = HttpParser::MAX_HEADER_SIZE);
    ~Connection();

    // 禁止拷贝 (socket 所有权唯一)
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // --- 生命周期 ---
    // 连接对象由 Server 的连接表 (shared_ptr) 持有, 一直存活到 Server::stop()
    // 统一销毁; 中继线程在流式期间另持一份引用。
    // 运行期不销毁对象, 是因为"无人引用"难以可证明地判定: worker (完成事件)、
    // 中继线程、扫描线程三方都可能持有裸指针, 而 ctx->user_data 指向对象、
    // 完成事件可能在任意时刻到达。实际尝试过三种销毁方案 (worker 侧摘除 /
    // 中继侧摘除 / 扫描线程统一摘除 + 在途计数), 都在压力下出现 use-after-free
    // (ASAN 实测), 因此改为保守策略: 只关闭连接 (fd 与缓冲立即释放),
    // 对象本身留到关机时统一销毁。
    // 代价: 对象常驻内存 (约百余字节/连接), 长跑会单调增长。
    // 彻底解决需要换所有权模型 (例如每连接一个由单一线程驱动的状态机,
    // 或把引用计数句柄随完成事件一起传递), 属后续工作。

    // --- 状态查询 ---
    socket_t fd() const noexcept { return fd_; }
    uint64_t id() const noexcept { return id_; }   // 连接表主键 (非 fd)
    bool is_closed() const noexcept { return fd_ == INVALID_SOCKET_VAL; }

    // 空闲时长 (毫秒, 自最后一次收发算起) — keep-alive 超时扫描用
    int64_t idle_ms() const;



    // --- SSE 中继标记 ---
    // 中继线程持有此连接期间为 true。worker 与空闲扫描线程据此跳过
    // 正在被中继使用的连接, 避免销毁仍被引用的对象 (use-after-free)。
    void mark_streaming() noexcept { streaming_.store(true, std::memory_order_release); }
    void unmark_streaming() noexcept { streaming_.store(false, std::memory_order_release); }
    bool is_streaming() const noexcept { return streaming_.load(std::memory_order_acquire); }

    // --- 事件处理 (由 Server/IOEngine 完成循环调用) ---
    void on_accept_complete();       // Accept 完成 → 投递第一个 recv
    void on_recv_complete(IOContext* ctx);   // 接收数据完成 → 喂入解析器
    void on_send_complete(IOContext* ctx);   // 发送完成 → 决定下一步
    void on_timeout();               // 空闲超时 → 关闭连接

    // --- 操作 ---
    void close();                    // 主动关闭连接
    void start_reading();            // 投递异步 recv

private:
    // --- 内部方法 ---

    // 处理完整的 HTTP 请求
    void handle_request();

    // 发送 HTTP 响应
    void send_response(HttpStatus status,
                       const std::string& content_type,
                       std::string_view body);

    // 发送错误响应
    void send_error(HttpStatus status, const char* message);

    // 构建 HTTP 响应字符串
    std::string build_http_response(HttpStatus status,
                                    const std::string& content_type,
                                    std::string_view body,
                                    bool keep_alive);

    // 同步发送原始字节 (用于 SSE 流式透传, 在 worker 线程中调用)
    // 循环 ::send() 直到全部发送或失败, 返回 true 表示成功
    bool send_raw_sync(const void* data, size_t len);

    // 转换到空闲状态 (keep-alive)
    void transition_to_idle();

    // --- 成员变量 ---
    uint64_t id_;                                // 唯一标识 (连接表主键)
    socket_t fd_ = INVALID_SOCKET_VAL;          // 套接字
    IOEngine* engine_;                           // I/O 引擎 (不拥有)
    BufferPool* pool_;                           // 缓冲池 (不拥有)
    Router* router_ = nullptr;                   // 路由器 (不拥有)
    RateLimiter* rate_limiter_ = nullptr;        // 限流器 (不拥有)
    std::string client_ip_;                      // 客户端 IP (用于 per-IP 限流)
    ConnectionState state_ = ConnectionState::ACCEPTED;
    HttpParser parser_;                          // HTTP 解析器状态机
    BufferHandle recv_buffer_;                   // 接收缓冲区 (RAII, 自动归还池)
    BufferHandle send_buffer_;                   // 发送缓冲区

    // 解析结果 (每次请求后重置)
    ParsedRequest current_request_;

    // 流水线数据: 当前请求解析完成后, buffer 中剩余的字节
    // (可能是下一个请求的开头, 不能丢弃)
    std::vector<uint8_t> pending_data_;

    // keep-alive 管理
    bool keep_alive_ = true;                     // 当前请求是否 keep-alive
    size_t request_count_ = 0;                   // 此连接上已处理的请求数
    size_t max_keepalive_requests_ = MAX_KEEPALIVE_REQUESTS;  // 来自配置

    // SSE 中继占用标记 (跨线程访问)
    std::atomic<bool> streaming_{false};

    // 时间戳 (用于超时管理)
    std::chrono::steady_clock::time_point last_activity_;
    std::chrono::steady_clock::time_point body_read_start_;  // body 读取开始时间
};

} // namespace gateway
} // namespace cyrus
