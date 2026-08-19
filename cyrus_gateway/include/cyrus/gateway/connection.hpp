// ============================================================================
// connection.hpp - 每连接状态机
// ============================================================================
// 管理单个 TCP 连接的生命周期:
//   ACCEPTED → READING_REQUEST → REQUEST_PARSED → SENDING_RESPONSE
//                                                    ↓
//                                   (keep-alive) → READING_REQUEST (循环)
//                                   (close)      → CLOSING → CLOSED
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
    REQUEST_PARSED,         // HTTP 请求解析完成, 待处理
    SENDING_RESPONSE,       // 正在发送 HTTP 响应
    IDLE,                   // 空闲 (keep-alive 模式, 等待下一个请求)
    CLOSING,                // 正在关闭
    CLOSED,                 // 已关闭
};

// ============================================================================
// Connection - 连接对象
// ============================================================================
class Connection {
    friend class Router;  // Router 需要访问 send_response/send_error
public:
    // --- 配置常量 ---
    static constexpr int    KEEPALIVE_TIMEOUT_MS   = 5000;    // keep-alive 空闲超时 (5秒)
    static constexpr int    BODY_READ_TIMEOUT_MS   = 30000;   // 请求体读取超时 (30秒)
    static constexpr size_t MAX_KEEPALIVE_REQUESTS = 1000;    // keep-alive 最大请求数
    static constexpr size_t RECV_BUFFER_SIZE       = 65536;   // 接收缓冲区大小 (64KB)

    Connection(socket_t fd, IOEngine* engine, BufferPool* pool,
               Router* router = nullptr, RateLimiter* rate_limiter = nullptr);
    ~Connection();

    // 禁止拷贝 (socket 所有权唯一)
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // --- 状态查询 ---
    ConnectionState state() const noexcept { return state_; }
    socket_t fd() const noexcept { return fd_; }
    bool keep_alive() const noexcept { return keep_alive_; }

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

    // 发送预构建的 SSE 完整响应 (HTTP header + SSE body)
    void send_sse_response(std::string_view full_response);

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

    // 请求 ID 生成器 (原子递增, 用于关联请求-响应)
    static std::atomic<uint64_t> s_next_request_id;

    // --- 成员变量 ---
    socket_t fd_ = INVALID_SOCKET_VAL;          // 套接字
    IOEngine* engine_;                           // I/O 引擎 (不拥有)
    BufferPool* pool_;                           // 缓冲池 (不拥有)
    Router* router_ = nullptr;                   // 路由器 (不拥有)
    RateLimiter* rate_limiter_ = nullptr;        // 限流器 (不拥有)
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

    // 时间戳 (用于超时管理)
    std::chrono::steady_clock::time_point last_activity_;
    std::chrono::steady_clock::time_point body_read_start_;  // body 读取开始时间
};

} // namespace gateway
} // namespace cyrus
