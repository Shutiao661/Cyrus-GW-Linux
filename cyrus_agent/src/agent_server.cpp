// ============================================================================
// agent_server.cpp - Agent TCP 服务器实现
// ============================================================================
// 使用简化的 select()-based 事件循环 (Agent 端负载轻, 不需要 IOCP 的全部能力)
// 支持:
//   - 多连接并发
//   - 二进制协议帧解码 (ProtocolDecoder)
//   - 请求路由 (按 URI 分发给不同的 RequestHandler)
// ============================================================================

#include "cyrus/agent/agent_server.hpp"
#include "cyrus/logger.hpp"

#include <algorithm>

namespace cyrus {
namespace agent {

// ============================================================================
// 构造/析构
// ============================================================================
AgentServer::AgentServer(uint16_t port, int worker_count)
    : port_(port)
    , worker_count_(worker_count)
{
}

AgentServer::~AgentServer() {
    stop();
}

// ============================================================================
// register_handler() - 注册请求处理器
// ============================================================================
void AgentServer::register_handler(const std::string& uri, RequestHandlerPtr handler) {
    handlers_[uri] = std::move(handler);
    LOG_INFO("Registered handler for: {}", uri);
}

// ============================================================================
// start() - 启动服务器
// ============================================================================
bool AgentServer::start() {
    // --- 第 1 步: 创建监听 socket ---
    listen_fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listen_fd_ == INVALID_SOCKET_VAL) {
        LOG_ERROR("AgentServer: socket() failed: {}", cyrus_socket_error());
        return false;
    }

    // 设置 SO_REUSEADDR
    int reuse = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR,
               reinterpret_cast<const char*>(&reuse), sizeof(reuse));

    // 设置非阻塞
    cyrus_set_nonblocking(listen_fd_);

    // --- 第 2 步: Bind ---
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);  // 仅本地
    addr.sin_port = htons(port_);

    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR_VAL) {
        LOG_ERROR("AgentServer: bind to port {} failed: {}", port_, cyrus_socket_error());
        cyrus_close_socket(listen_fd_);
        listen_fd_ = INVALID_SOCKET_VAL;
        return false;
    }

    // --- 第 3 步: Listen ---
    if (listen(listen_fd_, SOMAXCONN) == SOCKET_ERROR_VAL) {
        LOG_ERROR("AgentServer: listen failed: {}", cyrus_socket_error());
        cyrus_close_socket(listen_fd_);
        listen_fd_ = INVALID_SOCKET_VAL;
        return false;
    }

    // --- 第 4 步: 启动事件循环线程 ---
    running_ = true;
    event_thread_ = std::thread(&AgentServer::event_loop, this);

    LOG_INFO("Agent server listening on 127.0.0.1:{}", port_);
    return true;
}

// ============================================================================
// wait_for_shutdown() - 等待关闭信号
// ============================================================================
void AgentServer::wait_for_shutdown() {
    while (g_running.load(std::memory_order_acquire) && running_) {
        cyrus_sleep_ms(500);
    }
    stop();
}

// ============================================================================
// stop() - 停止服务器
// ============================================================================
void AgentServer::stop() {
    running_ = false;

    // 关闭监听 socket (唤醒 select)
    if (listen_fd_ != INVALID_SOCKET_VAL) {
        cyrus_close_socket(listen_fd_);
        listen_fd_ = INVALID_SOCKET_VAL;
    }

    // 关闭所有客户端连接
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        for (auto& [fd, state] : clients_) {
            cyrus_close_socket(fd);
        }
        clients_.clear();
    }

    if (event_thread_.joinable()) {
        event_thread_.join();
    }

    LOG_INFO("Agent server stopped");
}

// ============================================================================
// event_loop() - 简化的事件循环 (使用 select)
// ============================================================================
// select() 维护两个集合:
//   - read_fds: 监听 socket + 所有已连接 socket
void AgentServer::event_loop() {
    LOG_INFO("Agent event loop started");

    while (running_) {
        fd_set read_fds;
        FD_ZERO(&read_fds);

        // 添加监听 socket
        if (listen_fd_ != INVALID_SOCKET_VAL) {
            FD_SET(listen_fd_, &read_fds);
        }

        socket_t max_fd = listen_fd_;

        // 添加所有客户端 socket
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            for (const auto& [fd, state] : clients_) {
                FD_SET(fd, &read_fds);
                if (fd > max_fd) max_fd = fd;
            }
        }

        // 设置超时 (1 秒, 允许定期检查 running_)
        timeval timeout{1, 0};

        // --- select 等待事件 ---
        int ready = select(static_cast<int>(max_fd) + 1, &read_fds, nullptr, nullptr, &timeout);

        if (ready < 0) {
            if (!running_) break;
            int err = cyrus_socket_error();
            if (err != CYRUS_EINTR) {
                LOG_ERROR("AgentServer: select error: {}", err);
            }
            continue;
        }

        if (ready == 0) {
            continue;  // 超时, 重新循环
        }

        // --- 检查监听 socket (新连接) ---
        if (listen_fd_ != INVALID_SOCKET_VAL && FD_ISSET(listen_fd_, &read_fds)) {
            handle_new_connection();
            ready--;
        }

        // --- 检查客户端 socket (数据到达) ---
        if (ready > 0) {
            // 收集需要处理的 fd (在锁内读取, 在锁外处理)
            std::vector<socket_t> ready_fds;
            {
                std::lock_guard<std::mutex> lock(clients_mutex_);
                for (const auto& [fd, state] : clients_) {
                    if (FD_ISSET(fd, &read_fds)) {
                        ready_fds.push_back(fd);
                    }
                }
            } // 释放锁 — handle_client_data() 内部会再次获取 clients_mutex_
            for (socket_t fd : ready_fds) {
                handle_client_data(fd);
            }
        }
    }

    LOG_INFO("Agent event loop exited");
}

// ============================================================================
// handle_new_connection() - 接受新连接
// ============================================================================
void AgentServer::handle_new_connection() {
    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);

    socket_t client_fd = accept(listen_fd_,
                                 reinterpret_cast<sockaddr*>(&client_addr),
                                 &addr_len);

    if (client_fd == INVALID_SOCKET_VAL) {
        int err = cyrus_socket_error();
        if (err != CYRUS_EWOULDBLOCK && running_) {
            LOG_ERROR("AgentServer: accept failed: {}", err);
        }
        return;
    }

    // 设置非阻塞
    cyrus_set_nonblocking(client_fd);

    // 记录客户端
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_[client_fd] = ClientState{};
    }

    LOG_DEBUG("AgentServer: new client fd={}", static_cast<int>(client_fd));
}

// ============================================================================
// handle_client_data() - 处理客户端数据
// ============================================================================
void AgentServer::handle_client_data(socket_t client_fd) {
    // 读取数据
    uint8_t buffer[65536];  // 64KB 栈缓冲区
    int bytes_read = recv(client_fd,
                          reinterpret_cast<char*>(buffer),
                          sizeof(buffer),
                          0);

    if (bytes_read <= 0) {
        if (bytes_read == 0) {
            LOG_DEBUG("AgentServer: client fd={} disconnected", static_cast<int>(client_fd));
        } else {
            int err = cyrus_socket_error();
            if (err != CYRUS_EWOULDBLOCK) {
                LOG_WARN("AgentServer: recv error on fd={}: {}", static_cast<int>(client_fd), err);
            } else {
                return;  // 无数据, 不是错误
            }
        }

        // 关闭此客户端
        cyrus_close_socket(client_fd);
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_.erase(client_fd);
        return;
    }

    // --- 喂入 ProtocolDecoder ---
    ClientState* state = nullptr;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto it = clients_.find(client_fd);
        if (it == clients_.end()) return;
        state = &it->second;
    }

    // 设置帧回调
    state->decoder.on_frame = [this, client_fd](
        const MessageHeader& header,
        const uint8_t* payload,
        size_t payload_len)
    {
        process_request_on_connection(client_fd, header, payload, payload_len);
    };

    // 喂入数据 (如果累积够一帧, 会立即触发回调)
    state->decoder.feed(buffer, static_cast<size_t>(bytes_read));
}

// ============================================================================
// process_request_on_connection() - 处理解码后的请求
// ============================================================================
void AgentServer::process_request_on_connection(
    socket_t client_fd,
    const MessageHeader& header,
    const uint8_t* payload,
    size_t payload_len)
{
    // 验证消息类型
    if (header.msg_type != MessageType::MSG_REQUEST) {
        LOG_WARN("AgentServer: unexpected msg_type={} from fd={}",
                 header.msg_type, static_cast<int>(client_fd));
        auto error_frame = ProtocolCodec::encode_error(
            header.request_id,
            ErrorCode::E_AGENT_PROTOCOL_ERROR,
            "Expected MSG_REQUEST");
        send(client_fd,
             reinterpret_cast<const char*>(error_frame.data()),
             static_cast<int>(error_frame.size()), 0);
        return;
    }

    // --- 解析请求 payload ---
    // 格式: [method_str][uri_str][header_count:u16][headers...][body_str]
    const uint8_t* ptr = payload;
    const uint8_t* end = payload + payload_len;

    // 辅助: 读取 length-prefixed string
    auto read_string = [&ptr, end]() -> std::string {
        if (ptr + sizeof(uint16_t) > end) return "";
        uint16_t len;
        std::memcpy(&len, ptr, sizeof(uint16_t));
        len = ntohs(len);
        ptr += sizeof(uint16_t);
        if (ptr + len > end) return "";
        std::string s(reinterpret_cast<const char*>(ptr), len);
        ptr += len;
        return s;
    };

    std::string method = read_string();
    std::string uri    = read_string();

    // 读取 headers (跳过头部的数量 * (name_len + value_len))
    if (ptr + sizeof(uint16_t) > end) return;
    uint16_t header_count;
    std::memcpy(&header_count, ptr, sizeof(uint16_t));
    header_count = ntohs(header_count);
    ptr += sizeof(uint16_t);
    for (uint16_t i = 0; i < header_count && ptr < end; ++i) {
        read_string();  // header name (跳过)
        read_string();  // header value (跳过)
    }

    // 读取 body
    std::string body = read_string();

    LOG_INFO("AgentServer: request fd={} {} {} (body: {} bytes)",
             static_cast<int>(client_fd), method, uri, body.size());

    // --- 查找处理器 ---
    auto handler_it = handlers_.find(uri);
    if (handler_it == handlers_.end()) {
        // 未知 URI: 返回错误
        LOG_WARN("AgentServer: no handler for URI: {}", uri);
        auto error_frame = ProtocolCodec::encode_error(
            header.request_id,
            ErrorCode::E_AGENT_INTERNAL_ERROR,
            std::format("No handler for: {}", uri));
        send(client_fd,
             reinterpret_cast<const char*>(error_frame.data()),
             static_cast<int>(error_frame.size()), 0);
        return;
    }

    // --- 调用处理器 ---
    AgentRequest req{method, uri, body};
    AgentResponse resp;

    try {
        resp = handler_it->second->handle(req);
    } catch (const std::exception& e) {
        // LLM 调用或其他异常 → 发送错误帧
        LOG_ERROR("AgentServer: handler exception for {}: {}", uri, e.what());
        auto error_frame = ProtocolCodec::encode_error(
            header.request_id,
            ErrorCode::E_AGENT_INTERNAL_ERROR,
            std::format("Handler error: {}", e.what()));
        send(client_fd,
             reinterpret_cast<const char*>(error_frame.data()),
             static_cast<int>(error_frame.size()), 0);
        // 不立即关闭, 等待 Gateway 收到 error 帧后主动断开
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            clients_.erase(client_fd);
        }
        cyrus_close_socket(client_fd);
        return;
    }

    // --- 发送响应 ---
    if (resp.stream) {
        // 流式响应: 发送多个数据帧
        // 先发送响应头
        auto headers_frame = ProtocolCodec::encode_response_headers(
            header.request_id,
            static_cast<uint16_t>(resp.status),
            {});

        if (send(client_fd,
                 reinterpret_cast<const char*>(headers_frame.data()),
                 static_cast<int>(headers_frame.size()), 0) == SOCKET_ERROR_VAL) {
            LOG_WARN("AgentServer: failed to send response headers to fd={}", static_cast<int>(client_fd));
            goto cleanup_client;
        }

        // 发送数据块
        for (size_t i = 0; i < resp.stream_chunks.size(); ++i) {
            bool is_last = (i == resp.stream_chunks.size() - 1);
            auto data_frame = ProtocolCodec::encode_response_data(
                header.request_id,
                resp.stream_chunks[i],
                is_last);
            if (send(client_fd,
                     reinterpret_cast<const char*>(data_frame.data()),
                     static_cast<int>(data_frame.size()), 0) == SOCKET_ERROR_VAL) {
                LOG_WARN("AgentServer: failed to send data chunk {}/{} to fd={}",
                         i + 1, resp.stream_chunks.size(), static_cast<int>(client_fd));
                goto cleanup_client;
            }
        }

        // 发送结束帧
        auto end_frame = ProtocolCodec::encode_response_end(header.request_id);
        send(client_fd,
             reinterpret_cast<const char*>(end_frame.data()),
             static_cast<int>(end_frame.size()), 0);
    } else {
        // 单帧响应
        auto headers_frame = ProtocolCodec::encode_response_headers(
            header.request_id,
            static_cast<uint16_t>(resp.status),
            {});

        if (send(client_fd,
                 reinterpret_cast<const char*>(headers_frame.data()),
                 static_cast<int>(headers_frame.size()), 0) == SOCKET_ERROR_VAL) {
            LOG_WARN("AgentServer: failed to send response headers to fd={}", static_cast<int>(client_fd));
            goto cleanup_client;
        }

        if (!resp.body.empty()) {
            auto data_frame = ProtocolCodec::encode_response_data(
                header.request_id,
                resp.body,
                true);  // is_last = true
            send(client_fd,
                 reinterpret_cast<const char*>(data_frame.data()),
                 static_cast<int>(data_frame.size()), 0);
        }

        auto end_frame = ProtocolCodec::encode_response_end(header.request_id);
        send(client_fd,
             reinterpret_cast<const char*>(end_frame.data()),
             static_cast<int>(end_frame.size()), 0);
    }

    // 修复: 等待所有数据帧和 MSG_RESPONSE_END 发送完成后再关闭连接
    // (之前版本的过早关闭会导致 Gateway 端接收不完整)
    LOG_DEBUG("AgentServer: response sent to fd={}, closing connection",
              static_cast<int>(client_fd));

cleanup_client:
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_.erase(client_fd);
    }
    cyrus_close_socket(client_fd);
}

} // namespace agent
} // namespace cyrus
