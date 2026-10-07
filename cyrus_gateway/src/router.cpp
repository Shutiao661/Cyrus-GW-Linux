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
    agent_connect_timeout_ms_ = config_.get_int("agent", "connect_timeout_ms", 5000);
}

Router::~Router() {
    // 先排空中继线程 (它们持有 Connection*/AgentClient* 裸指针),
    // 再释放 Agent 连接池, 避免 use-after-free
    drain_relays();
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
        if (connect_agent(client.get())) {
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

// 用配置的连接超时连接 Agent (避免对端不可达时阻塞 worker/中继线程)
bool Router::connect_agent(AgentClient* client) {
    return client->connect(agent_host_, agent_port_, agent_connect_timeout_ms_);
}

AgentClient* Router::acquire_agent_client() {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    if (agent_pool_.empty()) {
        return nullptr;
    }

    // Round-robin 轮询 + 健康检查 + 忙标记 (跳过已断开/已占用的连接)
    size_t pool_size = agent_pool_.size();
    for (size_t attempt = 0; attempt < pool_size; ++attempt) {
        size_t index = next_agent_index_ % pool_size;
        next_agent_index_++;

        auto& client = agent_pool_[index];

        // 已被其他流式请求占用 → 跳过 (串行协议不能并发复用同一 socket)
        if (client->is_in_use()) {
            continue;
        }

        // fd 有效 且 对端未关闭 → 可复用。
        // 必须探活: Agent 处理完一个请求就关闭连接, 而 TCP 半关闭在本地
        // 看不出来, 只看 is_connected() 会一直派发死连接。
        if (client->is_connected() && !client->peer_closed()) {
            // 原子抢占使用权; 失败说明发生并发竞争, 尝试下一个
            if (client->try_acquire()) {
                return client.get();
            }
            continue;
        }

        // 连接已断开, 尝试重连
        LOG_WARN("Agent client #{} disconnected, attempting reconnect to {}:{}",
                 index, agent_host_, agent_port_);
        if (connect_agent(client.get())) {
            LOG_INFO("Agent client #{} reconnected successfully", index);
            if (client->try_acquire()) {
                return client.get();
            }
        }
        LOG_WARN("Agent client #{} reconnect failed", index);
    }

    // 所有连接都不健康或都被占用
    LOG_WARN("No free agent connection (pool={} all busy or unhealthy)", pool_size);
    return nullptr;
}

void Router::release_agent_client(AgentClient* client) {
    // 连接保持打开供复用 (长连接池模式)
    // 如果连接已断开, 在下次 acquire 时自动重连
    if (client) {
        client->mark_idle();  // 释放忙标记, 允许其他请求复用
    }

    // 定期健康检查: 每 100 次 release 触发一轮心跳
    // 使用原子成员 (非 static 局部变量), 避免多线程下的数据竞争
    if (release_counter_.fetch_add(1, std::memory_order_acq_rel) % 100 == 99) {
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

        // 正在被流式请求占用 → 视为健康, 不触碰 (避免破坏进行中的流)
        if (client->is_in_use()) {
            healthy++;
            continue;
        }

        if (client->is_connected() && !client->peer_closed()) {
            healthy++;
        } else {
            unhealthy++;
            // 尝试后台重连
            if (connect_agent(client.get())) {
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
    // 计算真实 uptime (秒)
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - start_time_).count();

    // 统计健康的 Agent 连接 (pool_mutex_ 保护, 与 acquire/health_check 一致)
    size_t healthy_agents = 0;
    size_t total_agents = 0;
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        total_agents = agent_pool_.size();
        for (const auto& client : agent_pool_) {
            // 占用中的连接正在服务流式请求, 视为健康
            if (client->is_in_use() || client->is_connected()) {
                healthy_agents++;
            }
        }
    }

    std::string json = std::format(
        R"({{"status":"ok","version":"2.0.0","platform":"Linux","engine":"{}","uptime_seconds":{},"active_relays":{},"agents":{{"healthy":{},"total":{}}}}})",
        CYRUS_HAS_LIBURING ? "io_uring" : "io_uring(POSIX)",
        uptime,
        active_relays_.load(std::memory_order_acquire),
        healthy_agents, total_agents
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
    // --- 第 1 步: 获取 Agent 连接 (带忙标记, 串行协议独占) ---
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
        release_agent_client(agent);
        conn->send_error(HttpStatus::BAD_GATEWAY,
            R"({"error":{"message":"Failed to communicate with agent","type":"gateway_error","code":502}})");
        return;
    }

    // --- 第 3 步: 分发到独立中继线程 (worker 线程立即返回, 继续服务其他连接) ---
    dispatch_relay(agent, conn, SSERelayTimeout::from_config(config_));
}

// ============================================================================
// dispatch_relay() - 将流式聊天请求分发到独立中继线程
// ============================================================================
// 有界并发: 超过 max_concurrent_relays_ 直接拒绝, 不排队 (SSE 长连接不排队,
// 避免内存/线程无界增长)。worker 线程调用, 立即返回。
void Router::dispatch_relay(AgentClient* agent, Connection* conn,
                            SSERelayTimeout sse_timeout) {
    {
        std::lock_guard<std::mutex> lock(relay_mutex_);

        if (relay_shutdown_.load(std::memory_order_acquire)) {
            release_agent_client(agent);
            conn->send_error(HttpStatus::SERVICE_UNAVAILABLE,
                R"({"error":{"message":"Server is shutting down","type":"gateway_error","code":503}})");
            return;
        }

        if (active_relays_.load(std::memory_order_acquire) >= max_concurrent_relays_) {
            LOG_WARN("Max concurrent SSE relays reached ({}), rejecting request",
                     max_concurrent_relays_);
            release_agent_client(agent);
            conn->send_error(HttpStatus::SERVICE_UNAVAILABLE,
                R"({"error":{"message":"Too many concurrent streams","type":"gateway_error","code":503}})");
            return;
        }

        active_relays_.fetch_add(1, std::memory_order_acq_rel);

        // 在 spawn 之前打上中继标记: dispatch_relay 返回后 worker 线程会检查
        // 这条连接能否销毁, 标记必须先于中继线程存在。
        // 中继线程整个生命周期持有一份引用。连接表项的摘除与对象销毁统一由
        // Server 的扫描线程负责 (条件: 已关闭 && 非中继中 && 无在途操作),
        // 中继这里既不摘表项也不销毁, 避免与 worker 的收尾竞争。
        std::shared_ptr<Connection> conn_ref = conn->shared_from_this();
        conn->mark_streaming();

        try {
            relay_threads_.emplace_back([this, agent, conn_ref, sse_timeout] {
                relay_stream(agent, conn_ref, sse_timeout);

                // 中继结束: 只清标记并归还引用; 销毁由扫描线程在确认无人引用后进行
                conn_ref->unmark_streaming();
                active_relays_.fetch_sub(1, std::memory_order_acq_rel);
                relay_cv_.notify_all();
            });
        } catch (const std::system_error& e) {
            conn->unmark_streaming();
            active_relays_.fetch_sub(1, std::memory_order_acq_rel);
            LOG_ERROR("Failed to spawn relay thread: {}", e.what());
            release_agent_client(agent);
            conn->send_error(HttpStatus::SERVICE_UNAVAILABLE,
                R"({"error":{"message":"Resource exhaustion","type":"gateway_error","code":503}})");
        }
    }
}

// ============================================================================
// relay_stream() - SSE 中继线程主体
// ============================================================================
// 在独立线程中阻塞收取 Agent 帧并实时推送给客户端。持有 Connection*/AgentClient*
// 裸指针, 生命周期由 Server/Router 保证 (排空在两者销毁之前完成)。
void Router::relay_stream(AgentClient* agent, std::shared_ptr<Connection> conn,
                          SSERelayTimeout sse_timeout) {
    // --- 初始化 SSE 中继器 ---
    SSERelayHandler relay(sse_timeout);
    LOG_DEBUG("SSE relay timeouts: first_byte={}ms total={}ms idle={}ms",
              sse_timeout.first_byte_timeout_ms,
              sse_timeout.total_timeout_ms,
              sse_timeout.idle_timeout_ms);

    // --- 立即发送 SSE HTTP header → 客户端可以开始接收数据 ---
    std::string sse_header = relay.build_sse_header();
    if (!conn->send_raw_sync(sse_header.data(), sse_header.size())) {
        LOG_ERROR("Failed to send SSE header, fd={}", static_cast<int>(conn->fd()));
        release_agent_client(agent);
        conn->close();
        return;
    }

    // --- ChunkedDecoder (按需启用) ---
    ChunkedDecoder chunked_decoder;
    bool use_chunked = false;  // 由 MSG_RESPONSE_HEADERS 决定

    chunked_decoder.on_data = [&relay](const uint8_t* data, size_t len) {
        relay.on_agent_data(std::string_view(reinterpret_cast<const char*>(data), len));
    };

    relay.on_send = [conn](std::string_view sse_data, bool /*is_last*/) {
        if (!sse_data.empty()) {
            conn->send_raw_sync(sse_data.data(), sse_data.size());
        }
    };

    // --- Agent 响应接收循环 (逐帧处理 → 实时推送) ---
    std::vector<uint8_t> frame_data;
    bool stream_ok = false;
    int consecutive_recv_failures = 0;
    constexpr int MAX_CONSECUTIVE_FAILURES = 5;  // 连续失败上限

    while (true) {
        // 排空标志: 关闭时尽快退出 (recv 最多阻塞 1s)
        if (relay_shutdown_.load(std::memory_order_acquire)) {
            LOG_DEBUG("SSE relay: shutdown requested, terminating");
            break;
        }

        if (!agent->recv_packet(frame_data, 1000)) {  // 1s 粒度 (仅控制线程阻塞)
            // recv 失败: 区分超时 (继续) vs 断开 (退出)
            if (!agent->is_connected()) {
                LOG_WARN("Agent connection lost during SSE relay");
                break;
            }
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
                relay.on_stream_complete();
                stream_ok = true;
                goto done_receiving;

            case MessageType::MSG_ERROR: {
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

    // --- 循环退出 (Agent 断开/超时/recv 连续失败/排空) ---
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

// ============================================================================
// drain_relays() - 优雅排空所有 SSE 中继线程
// ============================================================================
// 设置关闭标志 → 取出所有线程 → join 直到全部结束。
// 必须在 Server 销毁 Connection 对象之前调用 (否则中继线程访问已释放内存)。
void Router::drain_relays() {
    relay_shutdown_.store(true, std::memory_order_release);

    std::vector<std::thread> threads;
    {
        std::lock_guard<std::mutex> lock(relay_mutex_);
        threads.swap(relay_threads_);
    }

    relay_cv_.notify_all();  // 唤醒潜在等待者 (保持接口一致)

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    LOG_INFO("SSE relay threads drained");
}

} // namespace gateway
} // namespace cyrus
