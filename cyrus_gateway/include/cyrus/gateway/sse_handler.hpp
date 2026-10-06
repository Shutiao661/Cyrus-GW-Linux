// ============================================================================
// sse_handler.hpp - SSE (Server-Sent Events) 流处理器
// ============================================================================
// 包含两个组件:
//   1. SSEHandler — SSE 解析器 (接收端, 解析 Agent 返回的 SSE 流)
//   2. SSERelayHandler — SSE 中继器 (发送端, 将 Agent 二进制帧转为 SSE
//      并发送给客户端, 管理超时和错误传播)
//
// SSE 消息格式:
//   event: <type>\n
//   data: <payload>\n
//   \n
// ============================================================================

#pragma once

#include "cyrus/types.hpp"
#include "cyrus/config.hpp"
#include "cyrus/sse_codec.hpp"
#include "cyrus/logger.hpp"

#include <string>
#include <functional>
#include <sstream>
#include <chrono>
#include <vector>
#include <cstring>

namespace cyrus {






    namespace gateway {

// ============================================================================
// SSEEvent - SSE 解析事件
// ============================================================================
struct SSEEvent {
    std::string data;       // 数据字段
    std::string event;      // 事件类型 (可选)
    std::string id;         // 事件 ID (可选)
    int64_t retry = 0;      // 重连间隔 (可选, 毫秒)

    bool is_empty() const { return data.empty() && event.empty(); }
};

// ============================================================================
// SSEHandler - SSE 流解析器 (接收端)
// ============================================================================
class SSEHandler {
public:
    using EventCallback = std::function<void(const SSEEvent& event)>;
    EventCallback on_event;

    size_t feed(const uint8_t* data, size_t len);
    void reset();

private:
    void process_line(const std::string& line);
    void dispatch_event();

    std::string accumulator_;
    SSEEvent current_event_;
    std::string data_buffer_;
};

// ============================================================================
// SSERelayTimeout - 三层超时配置
// ============================================================================
struct SSERelayTimeout {
    int first_byte_timeout_ms = 15000;   // 首包超时: SSE header 后首次收到数据
    int total_timeout_ms      = 120000;  // 总超时: 整个请求从开始到结束
    int idle_timeout_ms       = 30000;   // 帧间超时: 两个数据块之间的最大间隔

    static SSERelayTimeout from_config(const Config& config) {
        SSERelayTimeout t;
        // 总预算: [sse] total_timeout_ms 优先; 未配置时回落到
        // [agent] request_timeout_ms (即"一次 Agent 请求的整体预算")
        int default_total = config.get_int("agent", "request_timeout_ms", 120000);
        t.first_byte_timeout_ms = config.get_int("sse", "first_byte_timeout_ms", 15000);
        t.total_timeout_ms      = config.get_int("sse", "total_timeout_ms", default_total);
        t.idle_timeout_ms       = config.get_int("sse", "idle_timeout_ms", 30000);
        return t;
    }
};

// ============================================================================
// SSERelayState - SSE 中继会话状态
// ============================================================================
enum class SSERelayState : uint8_t {
    INIT,               // 初始: 已发送 SSE HTTP header
    WAITING_FIRST_BYTE, // 等待 Agent 的第一个数据帧
    STREAMING,          // 正在中继数据帧
    ERROR_SENT,         // 已发送错误事件, 即将关闭
    DONE,               // 流完成 (data: [DONE] 已发送)
};

// ============================================================================
// SSERelayHandler - SSE 中继器 (发送端)
// ============================================================================
// 管理 Gateway→Client 的 SSE 中继会话:
//   1. 接收 Agent 的二进制协议帧 (通过 on_agent_data 回调)
//   2. 转换为 SSE 文本格式
//   3. 通过回调异步发送给客户端 (不与具体 socket 绑定)
//   4. 管理三层超时 (首包/总/帧间)
//   5. 在已发送 SSE header 后, 通过 event: error 传播下游错误
//
// 使用方式:
//   SSERelayHandler relay(timeout_config);
//   relay.on_send = [](const std::string& sse_data) { /* send to client */ };
//   relay.on_close = []() { /* cleanup */ };
//   relay.send_sse_header();  // 发送 Content-Type: text/event-stream
//   relay.on_agent_data(data, len);  // Agent 返回的二进制帧数据
//   relay.check_timeout();    // 每个事件循环周期检查超时
// ============================================================================
class SSERelayHandler {
public:
    // 发送回调: (SSE 格式化后的文本, 是否最后一块)
    using SendCallback = std::function<void(std::string_view sse_data, bool is_last)>;
    SendCallback on_send;

    // 关闭回调: 会话结束 (正常/异常)
    using CloseCallback = std::function<void()>;
    CloseCallback on_close;

    explicit SSERelayHandler(const SSERelayTimeout& timeout = SSERelayTimeout{})
        : timeout_(timeout)
    {
        session_start_ = std::chrono::steady_clock::now();
    }

    // --- 生命周期 ---

    // 发送 SSE HTTP 响应头
    // 调用此方法后, 后续错误只能通过 event: error 传播
    std::string build_sse_header() {
        state_ = SSERelayState::WAITING_FIRST_BYTE;
        first_byte_timer_ = std::chrono::steady_clock::now();

        std::string header;
        header += "HTTP/1.1 200 OK\r\n";
        header += "Server: Cyrus-GW/2.0\r\n";
        header += "Content-Type: text/event-stream\r\n";
        header += "Cache-Control: no-cache\r\n";
        header += "Connection: keep-alive\r\n";
        header += "X-Accel-Buffering: no\r\n";  // 禁用 nginx 代理缓冲
        header += "\r\n";
        return header;
    }

    // 喂入 Agent 返回的数据 (二进制帧的 payload 部分, 已解码)
    // 每调用一次表示收到一个 Agent 数据块 (token)
    void on_agent_data(std::string_view sse_chunk) {
        last_data_time_ = std::chrono::steady_clock::now();

        if (state_ == SSERelayState::WAITING_FIRST_BYTE) {
            state_ = SSERelayState::STREAMING;
            LOG_DEBUG("SSE relay: first byte received after {}ms",
                      elapsed_ms(first_byte_timer_));
        }

        // 透传 SSE chunk 到客户端
        if (on_send && !sse_chunk.empty()) {
            on_send(sse_chunk, false);
        }
    }

    // 流正常完成
    void on_stream_complete() {
        if (state_ == SSERelayState::DONE || state_ == SSERelayState::ERROR_SENT) return;

        state_ = SSERelayState::DONE;
        std::string done_msg = SSEFormatter::stream_done();
        if (on_send) {
            on_send(done_msg, true);
        }
        LOG_DEBUG("SSE relay: stream completed ({}ms total)",
                  elapsed_ms(session_start_));
        if (on_close) on_close();
    }

    // Agent 返回错误帧 → 通过 event: error 传播
    void on_agent_error(std::string_view error_message, int http_status = 502) {
        send_error_event("upstream_error", error_message, http_status);
    }

    // --- 超时检查 (每个事件循环周期调用) ---

    // 返回 true 表示会话仍然活跃, false 表示已关闭 (超时触发)
    bool check_timeout() {
        if (state_ == SSERelayState::DONE || state_ == SSERelayState::ERROR_SENT) {
            return false;
        }

        auto now = std::chrono::steady_clock::now();

        // 首包超时: SSE header 发送后, 在 first_byte_timeout_ms 内未收到任何数据
        if (state_ == SSERelayState::WAITING_FIRST_BYTE) {
            if (elapsed_ms(first_byte_timer_, now) > timeout_.first_byte_timeout_ms) {
                LOG_WARN("SSE relay: first byte timeout ({}ms)",
                         timeout_.first_byte_timeout_ms);
                send_error_event("upstream_first_byte_timeout",
                    "Agent did not respond within timeout", 504);
                return false;
            }
        }

        // 总超时: 从会话开始到现在
        if (elapsed_ms(session_start_, now) > timeout_.total_timeout_ms) {
            LOG_WARN("SSE relay: total timeout ({}ms)", timeout_.total_timeout_ms);
            send_error_event("request_total_timeout",
                "Request exceeded total time limit", 504);
            return false;
        }

        // 帧间超时: STREAMING 状态下, 两个数据帧之间的间隔
        if (state_ == SSERelayState::STREAMING) {
            auto idle_ms = elapsed_ms(last_data_time_, now);
            if (idle_ms > timeout_.idle_timeout_ms) {
                LOG_WARN("SSE relay: idle timeout ({}ms, max {}ms)",
                         idle_ms, timeout_.idle_timeout_ms);
                send_error_event("upstream_stream_stalled",
                    "Agent stream stalled", 504);
                return false;
            }
        }

        return true;
    }

    // --- 状态查询 ---
    SSERelayState state() const noexcept { return state_; }
    bool is_active() const noexcept {
        return state_ == SSERelayState::WAITING_FIRST_BYTE ||
               state_ == SSERelayState::STREAMING;
    }

private:
    // 发送 event: error 到客户端 (OpenAI 兼容格式)
    void send_error_event(std::string_view error_type,
                          std::string_view error_message,
                          int http_status)
    {
        if (state_ == SSERelayState::ERROR_SENT || state_ == SSERelayState::DONE) {
            return;  // 已经发送过错误或完成了
        }

        state_ = SSERelayState::ERROR_SENT;

        // OpenAI 兼容的错误格式
        std::ostringstream oss;
        oss << "event: error\n";
        oss << "data: {\"error\":{\"message\":\"" << error_message
            << "\",\"type\":\"" << error_type
            << "\",\"code\":" << http_status << "}}\n\n";

        std::string error_sse = oss.str();
        LOG_DEBUG("SSE relay: sending error event: {}", error_sse);

        if (on_send) {
            on_send(error_sse, true);  // is_last = true, 客户端可关闭
        }
        if (on_close) on_close();
    }

    static int64_t elapsed_ms(const std::chrono::steady_clock::time_point& start) {
        return elapsed_ms(start, std::chrono::steady_clock::now());
    }

    static int64_t elapsed_ms(const std::chrono::steady_clock::time_point& start,
                               const std::chrono::steady_clock::time_point& end) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    }

    SSERelayTimeout timeout_;
    SSERelayState state_ = SSERelayState::INIT;

    std::chrono::steady_clock::time_point session_start_;
    std::chrono::steady_clock::time_point first_byte_timer_;
    std::chrono::steady_clock::time_point last_data_time_;
};

} // namespace gateway
} // namespace cyrus
