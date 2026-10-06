// ============================================================================
// agent_client.hpp - Agent TCP 客户端
// ============================================================================
// 管理与后端 Cyrus Agent 的 TCP 连接。
// 使用二进制协议帧 (ProtocolCodec/ProtocolDecoder) 进行通信。
//
// 工作流程:
//   1. 连接到 Agent (TCP, 127.0.0.1:9999)
//   2. 发送编码的请求帧
//   3. 由 Router 的中继线程同步 recv_packet() 收取响应帧
//
// 连接复用: 一个 AgentClient 可以被多个请求复用 (串行)
// 连接池: Router 维护多个 AgentClient 实例 (round-robin 负载均衡)
// ============================================================================

#pragma once

#include "cyrus/types.hpp"
#include "cyrus/protocol.hpp"

#include <string>
#include <atomic>

namespace cyrus {
namespace gateway {

class AgentClient {
public: 
    AgentClient() = default;
    ~AgentClient();

    // 禁止拷贝
    AgentClient(const AgentClient&) = delete;
    AgentClient& operator=(const AgentClient&) = delete;

    // --- 连接管理 ---

    // 连接到 Agent 服务器
    // host: 服务器地址 (如 "127.0.0.1")
    // port: 服务器端口 (如 9999)
    // timeout_ms: 连接超时 (来自 [agent] connect_timeout_ms), <=0 表示无限等待
    // 返回 true 表示连接成功
    bool connect(const std::string& host, int port, int timeout_ms = 5000);

    // 断开连接
    void disconnect();

    // 是否已连接
    bool is_connected() const noexcept { return fd_ != INVALID_SOCKET_VAL; }

    // 对端是否已关闭 (探活)。
    // Agent 采用"每连接一个请求-响应周期"的模型, 处理完就关闭连接, 而 TCP
    // 半关闭在本地 fd 上看不出来 —— 只查 is_connected() 会把死连接反复派发
    // 出去, 池子轮过一圈后每个请求都失败。用 MSG_PEEK 非阻塞探测:
    // 不消费数据, 返回 0 表示对端已发 FIN。
    bool peer_closed() const {
        if (fd_ == INVALID_SOCKET_VAL) return true;
        char c = 0;
        ssize_t n = ::recv(fd_, &c, 1, MSG_PEEK | MSG_DONTWAIT);
        if (n == 0) return true;                       // 对端已关闭
        if (n < 0) {
            return !(errno == EAGAIN || errno == EWOULDBLOCK);  // 无数据可读=正常
        }
        return false;                                  // 有数据可读 = 存活
    }

    // --- 忙标记 (并发独占) ---
    // 一个 AgentClient 的底层 socket 是串行协议, 同一时刻只能服务一个流式请求。
    // try_acquire() 用 CAS 原子地抢占使用权; 失败表示正被其他请求占用。
    bool try_acquire() noexcept {
        bool expected = false;
        return in_use_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire);
    }

    // 归还使用权 (供下一个请求复用)
    void mark_idle() noexcept { in_use_.store(false, std::memory_order_release); }

    // 是否正被占用
    bool is_in_use() const noexcept { return in_use_.load(std::memory_order_acquire); }

    // --- 数据收发 ---

    // 发送二进制协议帧 (阻塞发送)
    // data: 编码后的帧数据 (含长度前缀)
    // 返回 true 表示发送成功
    bool send_packet(const std::vector<uint8_t>& data);

    // 接收响应 (阻塞, 带超时)
    // out_frame: 输出解码后的帧 (payload only)
    // timeout_ms: 超时时间 (毫秒)
    // 返回 true 表示接收成功
    bool recv_packet(std::vector<uint8_t>& out_frame, int timeout_ms = 5000);

private:
    socket_t fd_ = INVALID_SOCKET_VAL;
    std::string host_;
    int port_ = 0;
    std::atomic<bool> in_use_{false};  // 并发独占标记 (串行协议复用保护)
};

} // namespace gateway
} // namespace cyrus
