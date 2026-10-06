// ============================================================================
// agent_server.hpp - Agent TCP 服务器
// ============================================================================
// 基于 select() 的 TCP 服务器, 接收 Gateway 转发的客户端请求。
//
// 架构:
//   1. 创建监听 socket → bind → listen
//   2. select() 事件循环监听新连接与数据到达
//   3. 接受连接后使用 ProtocolDecoder 解析二进制协议帧
//   4. 解码完整的请求帧 → 分发给 RequestHandler 处理
//   5. 将处理结果编码为响应帧 → send 返回给 Gateway
//   6. 发送完成后关闭连接
//
// 与 Gateway 的区别:
//   - Agent 使用简化的单连接处理模型 (每连接一个请求-响应周期)
//   - Agent 不需要 HTTP 解析 (使用二进制协议帧)
//   - Agent 不需要 keep-alive (每个请求独立连接)
// ============================================================================

#pragma once

#include "cyrus/types.hpp"
#include "cyrus/protocol.hpp"
#include "request_handler.hpp"

#include <memory>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <vector>

namespace cyrus {
namespace agent {

class AgentServer {
public:
    explicit AgentServer(uint16_t port);
    ~AgentServer();

    // 禁止拷贝
    AgentServer(const AgentServer&) = delete;
    AgentServer& operator=(const AgentServer&) = delete;

    // --- 生命周期 ---

    // 启动服务器 (创建 socket, bind, listen, 启动事件循环)
    bool start();

    // 等待关闭信号
    void wait_for_shutdown();

    // 停止服务器
    void stop();

    // --- 请求处理器注册 ---
    void register_handler(const std::string& uri, RequestHandlerPtr handler);

private:
    // --- 网络配置 ---
    uint16_t port_;

    // --- 监听 ---
    socket_t listen_fd_ = INVALID_SOCKET_VAL;

    // --- 请求处理器 ---
    // 按 URI 路由: /v1/chat/completions → ChatHandler, /echo → EchoHandler
    std::unordered_map<std::string, RequestHandlerPtr> handlers_;

    // --- 事件循环 ---
    // 简化实现: 使用单线程 select()-based 事件循环
    // Agent 端负载较轻, 不需要 io_uring
    void event_loop();
    void handle_new_connection();
    void handle_client_data(socket_t client_fd);
    void process_request_on_connection(socket_t client_fd,
                                       const MessageHeader& header,
                                       const uint8_t* payload,
                                       size_t payload_len);

    bool running_ = false;
    std::thread event_thread_;

    // --- 每连接解码器状态 ---
    struct ClientState {
        ProtocolDecoder decoder;
        // 存储解码后的消息
    };
    std::unordered_map<socket_t, ClientState> clients_;
    std::mutex clients_mutex_;
};

} // namespace agent
} // namespace cyrus
