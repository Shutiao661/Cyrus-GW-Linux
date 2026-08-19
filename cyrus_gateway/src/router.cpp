// ============================================================================
// router.cpp - 路由器实现
// ============================================================================

#include "cyrus/gateway/router.hpp"
#include "cyrus/gateway/connection.hpp"
#include "cyrus/gateway/sse_handler.hpp"
#include "cyrus/chunked_decoder.hpp"
#include "cyrus/logger.hpp"

#include <sstream>
#include <cstring>

namespace cyrus {
namespace gateway {

// ============================================================================
// 构造/析构
// ============================================================================
Router::Router(const Config& config) : config_(config) {
    agent_host_      = config_.get("agent", "host", "127.0.0.1");
    agent_port_      = config_.get_int("agent", "port", 9999);
    agent_pool_size_ = config_.get_int("agent", "pool_size", 4);
}

Router::~Router() {
    // Agent 连接通过 unique_ptr 自动关闭
}

// ============================================================================
// init() - 初始化路由表和 Agent 连接池
// ============================================================================
bool Router::init() {
    // --- 注册路由 ---
    routes_ = {
        { HttpMethod::GET,  "/",                   [this](auto* c, auto& r) { handle_welcome(c, r); } },
        { HttpMethod::GET,  "/health",             [this](auto* c, auto& r) { handle_health(c, r); } },
        { HttpMethod::POST, "/v1/chat/completions", [this](auto* c, auto& r) { handle_chat_completion(c, r); } },
        { HttpMethod::POST, "/v1/chat",             [this](auto* c, auto& r) { handle_chat_completion(c, r); } },
    };

    // --- 创建 Agent 连接池 ---
    LOG_INFO("Connecting to Agent backend at {}:{} (pool: {})",
             agent_host_, agent_port_, agent_pool_size_);

    agent_pool_.reserve(agent_pool_size_);
    for (int i = 0; i < agent_pool_size_; ++i) {
        auto client = std::make_unique<AgentClient>();
        if (client->connect(agent_host_, agent_port_)) {
            LOG_DEBUG("Agent client #{} connected", i);
            agent_pool_.push_back(std::move(client));
        } else {
            LOG_WARN("Agent client #{} failed to connect", i);
        }
    }

    if (agent_pool_.empty()) {
        LOG_WARN("No agent connections available (agent may not be running)");
        // 不致命: 网关仍可处理 /health 等不依赖 Agent 的路由
    }

    LOG_INFO("Router initialized: {} routes, {} agent connections",
             routes_.size(), agent_pool_.size());
    return true;
}

// ============================================================================
// route() - 路由请求
// ============================================================================
bool Router::route(Connection* conn, const ParsedRequest& request) {
    for (const auto& route : routes_) {
        if (route.method == request.method && route.path == request.uri) {
            route.handler(conn, request);
            return true;
        }
    }
    // 未匹配 → 404
    handle_404(conn, request);
    return false;
}

// ============================================================================
// Agent 连接池管理
// ============================================================================
AgentClient* Router::acquire_agent_client() {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    if (agent_pool_.empty()) {
        return nullptr;
    }

    // Round-robin 轮询 + 健康检查: 跳过已断开或标记为不健康的连接
    size_t pool_size = agent_pool_.size();
    for (size_t attempt = 0; attempt < pool_size; ++attempt) {
        size_t index = next_agent_index_ % pool_size;
        next_agent_index_++;

        auto& client = agent_pool_[index];
        if (client->is_connected()) {
            return client.get();
        }

        // 连接已断开, 尝试重连
        LOG_WARN("Agent client #{} disconnected, attempting reconnect to {}:{}",
                 index, agent_host_, agent_port_);
        if (client->connect(agent_host_, agent_port_)) {
            LOG_INFO("Agent client #{} reconnected successfully", index);
            return client.get();
        }
        LOG_WARN("Agent client #{} reconnect failed", index);
    }

    // 所有连接都不健康
    LOG_ERROR("All {} agent connections are unhealthy", pool_size);
    return nullptr;
}

void Router::release_agent_client(AgentClient* client) {
    // 连接保持打开供复用 (长连接池模式)
    // 如果连接已断开, 在下次 acquire 时自动重连
    (void)client;

    // 定期健康检查: 每 100 次 release 触发一轮心跳
    static int release_counter = 0;
    if (++release_counter % 100 == 0) {
        health_check();
    }
}

// 后台健康检查: 向所有 Agent 连接发送心跳, 标记不健康连接
void Router::health_check() {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    int healthy = 0;
    int unhealthy = 0;

    for (size_t i = 0; i < agent_pool_.size(); ++i) {
        auto& client = agent_pool_[i];
        if (client->is_connected()) {
            healthy++;
        } else {
            unhealthy++;
            // 尝试后台重连
            if (client->connect(agent_host_, agent_port_)) {
                healthy++;
                unhealthy--;
                LOG_INFO("Agent client #{} reconnected during health check", i);
            }
        }
    }

    if (unhealthy > 0) {
        LOG_WARN("Agent pool health: {} healthy, {} unhealthy (of {} total)",
                 healthy, unhealthy, agent_pool_.size());
    }
}

// ============================================================================
// 路由处理函数
// ============================================================================

// --- 404 Not Found ---
void Router::handle_404(Connection* conn, const ParsedRequest& request) {
    std::string body = std::format("404 Not Found: {} {}\n",
                                    http_method_to_sv(request.method),
                                    request.uri);
    conn->send_error(HttpStatus::NOT_FOUND, body.c_str());
    LOG_DEBUG("404: {} {}", http_method_to_sv(request.method), request.uri);
}

// --- 欢迎页面 ---
void Router::handle_welcome(Connection* conn, const ParsedRequest& request) {
    const char* html = R"(<!DOCTYPE html>
<html>
<head><title>Cyrus-GW</title><meta charset="utf-8">
<style>body{font-family:sans-serif;max-width:700px;margin:40px auto;padding:0 20px;line-height:1.6}
h1{color:#333}code{background:#f4f4f4;padding:2px 6px;border-radius:3px}
.endpoint{background:#f8f8f8;padding:8px 12px;margin:4px 0;border-left:3px solid #4CAF50}
</style></head>
<body>
<h1> Cyrus-GW Gateway</h1>
<p>High-performance async HTTP gateway powered by C++20 + io_uring</p>
<h2>Endpoints</h2>
<div class="endpoint"><code>GET /health</code> — Health check</div>
<div class="endpoint"><code>POST /v1/chat/completions</code> — Chat completion (SSE streaming)</div>
<p><small>Platform: Linux | Engine: io_uring | Version: 2.0.0</small></p>
</body>
</html>)";
    conn->send_response(HttpStatus::OK, "text/html; charset=utf-8", html);
}

// --- 健康检查 ---
void Router::handle_health(Connection* conn, const ParsedRequest& request) {
    std::string json = std::format(
        R"({{"status":"ok","version":"2.0.0","platform":"{}","engine":"{}","uptime":"ok"}})",
#if CYRUS_PLATFORM_WINDOWS
        "Windows", "IOCP"
#else
        "Linux", "io_uring"
#endif
    );
    conn->send_response(HttpStatus::OK, "application/json", json);
}

// --- Chat Completion (流式透传到 Agent + SSE 实时推送给客户端) ---
// ============================================================================
// 流程:
//   1. 获取 Agent 连接 → 编码并发送请求
//   2. 立即发送 SSE HTTP header 给客户端 (实现真正的流式, 降低首字节延迟)
//   3. 接收 Agent 响应帧  (MSG_RESPONSE_HEADERS / MSG_RESPONSE_DATA / ...)
//   4. 若 Agent 使用 chunked TE → ChunkedDecoder 剥离长度行, 输出纯净 SSE
//   5. 每收到数据块立即通过 send_raw_sync() 推送给客户端
//   6. 三层超时检测: 首包 / 总耗时 / 帧间空闲
//   7. SSE header 已发送后 → 错误通过 event: error 传递 (OpenAI 兼容格式)
//   8. 流结束后关闭连接 (SSE 长连接不复用)
// ============================================================================
void Router::handle_chat_completion(Connection* conn, const ParsedRequest& request) {
    // --- 第 1 步: 获取 Agent 连接 ---
    AgentClient* agent = acquire_agent_client();
    if (!agent) {
        LOG_ERROR("No agent available for chat completion");
        conn->send_error(HttpStatus::SERVICE_UNAVAILABLE,
            R"({"error":{"message":"No agent backend available","type":"gateway_error","code":503}})");
        return;
    }

    // --- 第 2 步: 编码并发送请求到 Agent ---
    std::string body_str(request.body.begin(), request.body.end());
    std::vector<std::pair<std::string, std::string>> headers;
    for (const auto& [name, value] : request.headers) {
        headers.emplace_back(name, value);
    }

    auto frame = ProtocolCodec::encode_request(
        1,  // request_id
        http_method_to_sv(request.method),
        request.uri,
        headers,
        body_str);

    if (!agent->send_packet(frame)) {
        LOG_ERROR("Failed to send request to agent");
        conn->send_error(HttpStatus::BAD_GATEWAY,
            R"({"error":{"message":"Failed to communicate with agent","type":"gateway_error","code":502}})");
        release_agent_client(agent);
        return;
    }

    // --- 第 3 步: 初始化 SSE 中继器 + 超时配置 ---
    SSERelayTimeout sse_timeout = SSERelayTimeout::from_config(config_);
    SSERelayHandler relay(sse_timeout);

    // --- 第 4 步: 立即发送 SSE HTTP header → 客户端可以开始接收数据 ---
    std::string sse_header = relay.build_sse_header();
    if (!conn->send_raw_sync(sse_header.data(), sse_header.size())) {
        LOG_ERROR("Failed to send SSE header, fd={}", static_cast<int>(conn->fd()));
        release_agent_client(agent);
        conn->close();
        return;
    }
    // SSE header 已发送 → 后续错误必须通过 event: error 传递

    // --- 第 5 步: 设置 ChunkedDecoder (按需启用) ---
    ChunkedDecoder chunked_decoder;
    bool use_chunked = false;  // 由 MSG_RESPONSE_HEADERS 决定

    // ChunkedDecoder 输出 → SSERelayHandler 输入
    chunked_decoder.on_data = [&relay](const uint8_t* data, size_t len) {
        std::string_view clean(reinterpret_cast<const char*>(data), len);
        relay.on_agent_data(clean);
    };

    // --- 第 6 步: SSERelayHandler 输出 → 直接推送给客户端 ---
    relay.on_send = [conn](std::string_view sse_data, bool /*is_last*/) {
        if (!sse_data.empty()) {
            conn->send_raw_sync(sse_data.data(), sse_data.size());
        }
    };
    relay.on_close = []() {
        // 流结束后的清理在 done_receiving 统一处理
    };

    // --- 第 7 步: Agent 响应接收循环 (逐帧处理 → 实时推送) ---
    std::vector<uint8_t> frame_data;
    bool stream_ok = false;
    int consecutive_recv_failures = 0;
    constexpr int MAX_CONSECUTIVE_FAILURES = 5;  // 连续失败上限

    while (true) {
        if (!agent->recv_packet(frame_data, 1000)) {  // 1s 粒度 (仅控制线程阻塞)
            // recv 失败: 区分超时 (继续) vs 断开 (退出)
            if (!agent->is_connected()) {
                LOG_WARN("Agent connection lost during SSE relay");
                break;
            }
            // 超时: 检查 relay 超时, 若未超时继续等待
            consecutive_recv_failures++;
            if (consecutive_recv_failures >= MAX_CONSECUTIVE_FAILURES) {
                LOG_WARN("Agent recv consecutive failures exceeded ({})", MAX_CONSECUTIVE_FAILURES);
                break;
            }
            if (!relay.check_timeout()) {
                // relay 已超时并发送了 event: error
                LOG_WARN("SSE relay timeout during Agent recv");
                goto done_receiving;
            }
            continue;
        }
        consecutive_recv_failures = 0;  // 成功收取, 重置计数

        // 验证帧完整性
        if (frame_data.size() < sizeof(MessageHeader)) {
            frame_data.clear();
            continue;
        }

        MessageHeader resp_header;
        std::memcpy(&resp_header, frame_data.data(), sizeof(MessageHeader));
        size_t payload_offset = sizeof(MessageHeader);

        // --- 按消息类型分发 ---
        switch (resp_header.msg_type) {

            case MessageType::MSG_RESPONSE_HEADERS: {
                // Agent 返回响应头: 检查错误标记 + 检测 Transfer-Encoding
                if (resp_header.flags & FrameFlags::FLAG_ERROR) {
                    relay.on_agent_error("Agent returned error status", 502);
                    goto done_receiving;
                }
                // 检测是否需要 ChunkedDecoder
                std::string_view headers_payload(
                    reinterpret_cast<const char*>(frame_data.data() + payload_offset),
                    frame_data.size() - payload_offset);
                if (headers_payload.find("chunked") != std::string_view::npos ||
                    headers_payload.find("CHUNKED") != std::string_view::npos) {
                    use_chunked = true;
                    LOG_DEBUG("SSE relay: Agent using chunked TE, enabling ChunkedDecoder");
                }
                break;
            }

            case MessageType::MSG_RESPONSE_DATA: {
                // Agent 返回流式数据块
                std::string_view chunk(
                    reinterpret_cast<const char*>(frame_data.data() + payload_offset),
                    frame_data.size() - payload_offset);

                if (use_chunked) {
                    // 通过 ChunkedDecoder 剥离 chunk 编码层
                    chunked_decoder.feed(
                        reinterpret_cast<const uint8_t*>(chunk.data()), chunk.size());
                    if (chunked_decoder.is_error()) {
                        LOG_ERROR("ChunkedDecoder error: {}", chunked_decoder.error_message());
                        relay.on_agent_error("Chunked decode error", 502);
                        goto done_receiving;
                    }
                } else {
                    // 直通: payload 即为纯净 SSE 文本
                    relay.on_agent_data(chunk);
                }
                break;
            }

            case MessageType::MSG_RESPONSE_END:
                // Agent 发送流结束标记
                relay.on_stream_complete();
                stream_ok = true;
                goto done_receiving;

            case MessageType::MSG_ERROR: {
                // Agent 返回显式错误帧
                std::string_view err_msg(
                    reinterpret_cast<const char*>(frame_data.data() + payload_offset),
                    frame_data.size() - payload_offset);
                relay.on_agent_error(err_msg, 502);
                goto done_receiving;
            }

            default:
                LOG_DEBUG("SSE relay: unknown msg_type={}", static_cast<int>(resp_header.msg_type));
                break;
        }

        frame_data.clear();

        // 每帧处理后检查超时
        if (!relay.check_timeout()) {
            LOG_WARN("SSE relay timeout after processing frame");
            goto done_receiving;
        }
    }

    // --- 第 8 步: 循环退出 (Agent 断开/超时/recv 连续失败) ---
    if (!stream_ok && relay.is_active()) {
        relay.on_agent_error("Agent connection lost or unresponsive", 504);
    }

done_receiving:
    release_agent_client(agent);

    // 流结束后关闭客户端连接 (SSE 长连接不复用)
    conn->close();

    LOG_INFO("Chat completion SSE relay ended (stream_ok={}, chunked={})",
             stream_ok, use_chunked);
}

} // namespace gateway
} // namespace cyrus
