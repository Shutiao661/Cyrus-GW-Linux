// ============================================================================
// agent_client.cpp - Agent TCP 客户端实现
// ============================================================================

#include "cyrus/gateway/agent_client.hpp"
#include "cyrus/logger.hpp"

#include <poll.h>  // 非阻塞 connect 等待可写

namespace cyrus {
namespace gateway {

AgentClient::~AgentClient() {
    disconnect();
}

// ============================================================================
// connect() - 连接到 Agent 服务器
// ============================================================================
bool AgentClient::connect(const std::string& host, int port, int timeout_ms) {
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

    // 非阻塞 connect + poll 超时: 对端不可达时 (防火墙丢包等) connect 可能
    // 阻塞很久, 而调用方是 worker / 中继线程, 不能被它拖住。
    int flags = ::fcntl(fd_, F_GETFL, 0);
    if (flags != -1) {
        ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
    }

    int rc = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc != 0 && errno != EINPROGRESS) {
        LOG_ERROR("AgentClient: connect to {}:{} failed: {}", host, port, cyrus_socket_error());
        disconnect();
        return false;
    }

    if (rc != 0) {
        // 等待可写 (= 连接建立或失败)
        struct pollfd pfd{};
        pfd.fd = fd_;
        pfd.events = POLLOUT;
        int pr = ::poll(&pfd, 1, timeout_ms > 0 ? timeout_ms : -1);
        if (pr == 0) {
            LOG_ERROR("AgentClient: connect to {}:{} timed out ({}ms)", host, port, timeout_ms);
            disconnect();
            return false;
        }
        if (pr < 0) {
            LOG_ERROR("AgentClient: connect poll failed: {}", cyrus_socket_error());
            disconnect();
            return false;
        }
        int so_err = 0;
        socklen_t len = sizeof(so_err);
        if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &so_err, &len) != 0 || so_err != 0) {
            LOG_ERROR("AgentClient: connect to {}:{} failed: {}", host, port, so_err);
            disconnect();
            return false;
        }
    }

    // 恢复阻塞模式: 后续 recv 依赖 MSG_WAITALL + SO_RCVTIMEO
    if (flags != -1) {
        ::fcntl(fd_, F_SETFL, flags);
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

    // 循环发送直到全部写入 (处理部分发送; agent socket 为阻塞模式)
    size_t sent_total = 0;
    while (sent_total < data.size()) {
        int n = ::send(fd_,
                       reinterpret_cast<const char*>(data.data() + sent_total),
                       static_cast<int>(data.size() - sent_total),
                       0);  // 无特殊标志

        if (n == SOCKET_ERROR_VAL) {
            int err = cyrus_socket_error();
            if (err == CYRUS_EINTR) {
                continue;  // 被信号中断, 重试
            }
            LOG_ERROR("AgentClient: send failed: {}", err);
            disconnect();  // 标记断开, 连接池下次 acquire 时重连
            return false;
        }
        if (n == 0) {
            LOG_ERROR("AgentClient: send returned 0 (connection closed)");
            disconnect();
            return false;
        }
        sent_total += static_cast<size_t>(n);
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
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // --- 第 1 步: 读取 4 字节长度前缀 ---
    uint32_t payload_len_net = 0;
    int result = ::recv(fd_,
                        reinterpret_cast<char*>(&payload_len_net),
                        sizeof(uint32_t),
                        MSG_WAITALL);  // 等待完整 4 字节

    if (result != sizeof(uint32_t)) {
        if (result == 0) {
            // 对端已关闭。Agent 是"每连接一个请求-响应周期"的模型, 处理完就关,
            // 因此这里必须标记断开 —— 否则这个 fd 仍是"有效"的, 连接池会把它
            // 反复派发出去, 池子轮过一圈后每个请求都会失败。
            LOG_DEBUG("AgentClient: Agent closed connection");
            disconnect();
        } else if (result == SOCKET_ERROR_VAL) {
            int err = cyrus_socket_error();
            if (err == CYRUS_EWOULDBLOCK || err == CYRUS_EINTR) {
                return false;  // 收包超时/被信号打断: 连接仍然可用, 不标记断开
            }
            LOG_ERROR("AgentClient: recv header failed: {}", err);
            disconnect();
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
        disconnect();  // 帧读了一半, 连接已不可复用
        return false;
    }

    return true;
}

} // namespace gateway
} // namespace cyrus
