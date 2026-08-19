// ============================================================================
// agent_client.cpp - Agent TCP 客户端实现
// ============================================================================

#include "cyrus/gateway/agent_client.hpp"
#include "cyrus/logger.hpp"

namespace cyrus {
namespace gateway {

AgentClient::~AgentClient() {
    disconnect();
}

// ============================================================================
// connect() - 连接到 Agent 服务器
// ============================================================================
bool AgentClient::connect(const std::string& host, int port) {
    host_ = host;
    port_ = port;

    // 创建 TCP socket
    fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd_ == INVALID_SOCKET_VAL) {
        LOG_ERROR("AgentClient: socket() failed: {}", cyrus_socket_error());
        return false;
    }

    // 解析地址
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        LOG_ERROR("AgentClient: invalid address: {}", host);
        disconnect();
        return false;
    }

    // 连接到服务器
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR_VAL) {
        LOG_ERROR("AgentClient: connect to {}:{} failed: {}",
                  host, port, cyrus_socket_error());
        disconnect();
        return false;
    }

    LOG_DEBUG("AgentClient: connected to {}:{}", host, port);
    return true;
}

// ============================================================================
// disconnect() - 断开连接
// ============================================================================
void AgentClient::disconnect() {
    if (fd_ != INVALID_SOCKET_VAL) {
        cyrus_close_socket(fd_);
        fd_ = INVALID_SOCKET_VAL;
        LOG_DEBUG("AgentClient: disconnected from {}:{}", host_, port_);
    }
}

// ============================================================================
// send_packet() - 发送二进制协议帧
// ============================================================================
// 使用阻塞 send (Agent 客户端场景中, 发送帧很小, 阻塞是合理的)
bool AgentClient::send_packet(const std::vector<uint8_t>& data) {
    if (!is_connected()) {
        LOG_ERROR("AgentClient: not connected");
        return false;
    }

    int result = ::send(fd_,
                        reinterpret_cast<const char*>(data.data()),
                        static_cast<int>(data.size()),
                        0);  // 无特殊标志

    if (result == SOCKET_ERROR_VAL) {
        LOG_ERROR("AgentClient: send failed: {}", cyrus_socket_error());
        return false;
    }

    if (static_cast<size_t>(result) != data.size()) {
        LOG_WARN("AgentClient: partial send: {}/{} bytes", result, data.size());
        // 简化: 不处理部分发送 (帧通常 < 4KB)
    }

    return true;
}

// ============================================================================
// recv_packet() - 接收响应帧
// ============================================================================
// 先读取 4 字节长度前缀, 再读取 payload
bool AgentClient::recv_packet(std::vector<uint8_t>& out_frame, int timeout_ms) {
    if (!is_connected()) {
        LOG_ERROR("AgentClient: not connected");
        return false;
    }

    // --- 设置接收超时 ---
#ifdef _WIN32
    DWORD timeout = static_cast<DWORD>(timeout_ms);
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO,
               reinterpret_cast<const char*>(&timeout), sizeof(timeout));
#else
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif

    // --- 第 1 步: 读取 4 字节长度前缀 ---
    uint32_t payload_len_net = 0;
    int result = ::recv(fd_,
                        reinterpret_cast<char*>(&payload_len_net),
                        sizeof(uint32_t),
                        MSG_WAITALL);  // 等待完整 4 字节

    if (result != sizeof(uint32_t)) {
        if (result == 0) {
            LOG_DEBUG("AgentClient: Agent closed connection");
        } else if (result == SOCKET_ERROR_VAL) {
            int err = cyrus_socket_error();
            if (err != CYRUS_EWOULDBLOCK && err != CYRUS_EINTR) {
                LOG_ERROR("AgentClient: recv header failed: {}", err);
            }
        }
        return false;
    }

    uint32_t payload_len = ntohl(payload_len_net);

    // 检查长度是否合理
    if (payload_len > 10 * 1024 * 1024) {  // 最大 10MB
        LOG_ERROR("AgentClient: payload too large: {} bytes", payload_len);
        return false;
    }

    // --- 第 2 步: 读取 payload ---
    out_frame.resize(payload_len);
    result = ::recv(fd_,
                    reinterpret_cast<char*>(out_frame.data()),
                    static_cast<int>(payload_len),
                    MSG_WAITALL);

    if (result != static_cast<int>(payload_len)) {
        LOG_ERROR("AgentClient: recv payload failed: expected {}, got {}",
                  payload_len, result);
        return false;
    }

    return true;
}

} // namespace gateway
} // namespace cyrus
