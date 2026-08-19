// ============================================================================
// protocol.hpp - 二进制协议编解码 (Gateway ↔ Agent 通信协议)
// ============================================================================
// 在 TCP 流上传输离散消息的帧协议:
//
//   帧格式:
//   ┌─────────────────┬──────────────────────────┐
//   │ 长度 (4 bytes)   │ 载荷 (length bytes)       │
//   │ 网络字节序 uint32 │ 任意二进制数据             │
//   └─────────────────┴──────────────────────────┘
//
//   长度字段使用网络字节序 (大端, htonl/ntohl), 确保跨平台兼容。
//   注意: 长度不包含自身的 4 字节, 仅表示载荷长度。
//
// 消息类型 (载荷的第一个字节):
//   MSG_REQUEST           (0x01)  Gateway → Agent: 转发客户端请求
//   MSG_RESPONSE_HEADERS  (0x02)  Agent → Gateway: 响应头 (状态码 + Headers)
//   MSG_RESPONSE_DATA     (0x03)  Agent → Gateway: 响应体数据块 (流式)
//   MSG_RESPONSE_END      (0x04)  Agent → Gateway: 响应结束标记
//   MSG_ERROR             (0x05)  Agent → Gateway: 错误信息
//
// 流式协议:
//   Gateway 发送 MSG_REQUEST → Agent 回复 1 个 MSG_RESPONSE_HEADERS +
//   零个或多个 MSG_RESPONSE_DATA + 1 个 MSG_RESPONSE_END
//
// 设计决策:
//   - 使用 4 字节长度前缀而非分隔符 (如 \r\n\r\n), 因为载荷可能包含任意二进制
//   - 长度在网络字节序中, 避免不同 CPU 端序的兼容问题
//   - 编码/解码分离: ProtocolCodec (单帧编解码) + ProtocolDecoder (流式解码)
// ============================================================================

#pragma once

#include "types.hpp"

#include <vector>
#include <cstring>
#include <optional>

namespace cyrus {

// ============================================================================
// 消息类型常量
// ============================================================================
namespace MessageType {
    constexpr uint8_t MSG_REQUEST           = 0x01;  // 客户端请求 (G→A)
    constexpr uint8_t MSG_RESPONSE_HEADERS  = 0x02;  // 响应头部 (A→G)
    constexpr uint8_t MSG_RESPONSE_DATA     = 0x03;  // 响应数据块 (A→G, 流式)
    constexpr uint8_t MSG_RESPONSE_END      = 0x04;  // 响应结束 (A→G)
    constexpr uint8_t MSG_ERROR             = 0x05;  // 错误响应 (A→G)
}

// 帧标志位
namespace FrameFlags {
    constexpr uint8_t FLAG_END_OF_STREAM = 0x01;  // 此帧是流的最后一帧
    constexpr uint8_t FLAG_ERROR         = 0x02;  // 此帧包含错误信息
}

// ============================================================================
// 帧头 (Frame Header) - 16 字节固定头
// ============================================================================
// 在原始协议中, 载荷前有 16 字节头:
//   [u32 total_len][u64 request_id][u8 msg_type][u8 flags][u16 reserved]
//
// 为简便起见, 当前实现使用简化的 4 字节长度前缀协议。
// 请求 ID、消息类型等信息编码在载荷体内。
// 此结构体保留用于将来的完整协议实现。
// ============================================================================

#pragma pack(push, 1)
struct MessageHeader {
    uint32_t total_len = 0;     // 总长度 (不含自身, 即 payload 长度)
    uint64_t request_id = 0;    // 请求 ID (关联请求和响应)
    uint8_t  msg_type = 0;      // 消息类型 (MessageType 常量)
    uint8_t  flags = 0;         // 标志位 (FrameFlags 常量)
    uint16_t reserved = 0;      // 保留字段 (对齐到 16 字节)
};
#pragma pack(pop)

static_assert(sizeof(MessageHeader) == 16, "MessageHeader must be 16 bytes");

// ============================================================================
// ProtocolCodec - 协议编解码器 (静态方法)
// ============================================================================
// 用于将结构化数据编码为帧, 或将帧解码为结构化数据。
// 所有方法都是静态的, 不保持状态。
// ============================================================================
class ProtocolCodec {
public:
    // --- 编码方法 (C++ 对象 → 字节流) ---

    // 编码客户端请求
    // request_id: 请求 ID (用于关联响应)
    // method: HTTP 方法字符串 (如 "POST")
    // uri: 请求 URI (如 "/v1/chat/completions")
    // headers: HTTP 头列表 (每对是 name, value)
    // body: 请求体
    static std::vector<uint8_t> encode_request(
        uint64_t request_id,
        std::string_view method,
        std::string_view uri,
        const std::vector<std::pair<std::string, std::string>>& headers,
        std::string_view body);

    // 编码响应头
    // request_id: 对应的请求 ID
    // status: HTTP 状态码
    // headers: 响应头列表
    static std::vector<uint8_t> encode_response_headers(
        uint64_t request_id,
        uint16_t status,
        const std::vector<std::pair<std::string, std::string>>& headers);

    // 编码响应数据块 (流式)
    // request_id: 对应的请求 ID
    // data: 数据块内容
    // is_last: 是否是最后一块 (设置 FLAG_END_OF_STREAM)
    static std::vector<uint8_t> encode_response_data(
        uint64_t request_id,
        std::string_view data,
        bool is_last = false);

    // 编码响应结束标记
    static std::vector<uint8_t> encode_response_end(uint64_t request_id);

    // 编码错误响应
    // request_id: 对应的请求 ID
    // error_code: Cyrus 错误码
    // error_message: 人类可读的错误描述
    static std::vector<uint8_t> encode_error(
        uint64_t request_id,
        ErrorCode error_code,
        std::string_view error_message);

private:
    // 将 MessageHeader + payload 组装为完整的帧字节序列
    static std::vector<uint8_t> build_frame(const MessageHeader& header,
                                            const uint8_t* payload,
                                            size_t payload_len);
};

// ============================================================================
// ProtocolDecoder - 流式协议解码器
// ============================================================================
// 处理 TCP 流的粘包/拆包问题。
// TCP 是字节流协议, 不保留消息边界。一个 recv 可能收到:
//   - 半个帧 (拆包)
//   - 一个完整帧 + 半个下个帧 (粘包)
//   - 多个帧粘在一起 (粘包)
// ProtocolDecoder 维护一个内部缓冲区, 累积数据直到能解析出完整帧。
//
// 使用方式:
//   ProtocolDecoder decoder;
//   decoder.on_frame = [](const MessageHeader& h, const uint8_t* data, size_t len) {
//       // 处理收到的帧
//   };
//   decoder.feed(recv_buffer, bytes_received);  // 喂入新收到的数据
// ============================================================================
class ProtocolDecoder {
public:
    // 帧回调: (header, payload_data, payload_length)
    // 注意: payload 指针仅在回调期间有效, 不要保存
    using FrameCallback = std::function<void(const MessageHeader& header,
                                              const uint8_t* payload,
                                              size_t payload_len)>;

    FrameCallback on_frame;

    // 喂入新数据
    // data: 新收到的字节
    // len: 字节数
    // 每次内部累积的数据足够解码一个完整帧时, 调用 on_frame 回调
    void feed(const uint8_t* data, size_t len) {
        buffer_.insert(buffer_.end(), data, data + len);

        while (try_decode()) {
            // 循环解码, 直到剩余数据不足以构成完整帧
        }
    }

    // 重置解码器状态 (用于连接复用)
    void reset() {
        buffer_.clear();
    }

    // 当前累积的未处理数据量
    size_t buffered_bytes() const {
        return buffer_.size();
    }

private:
    // 尝试从 buffer_ 中解码一个帧
    // 返回 true 表示成功解码一帧, 可以继续尝试
    // 返回 false 表示数据不足一帧 (需要更多数据)
    bool try_decode() {
        // 需要至少完整头的大小: 先检查是否有 4 字节 (简化协议的长度前缀)
        // 完整协议: 需要至少 sizeof(MessageHeader) = 16 字节
        if (buffer_.size() < sizeof(uint32_t)) {
            return false;  // 连长度前缀都不完整
        }

        // 读取 4 字节长度 (网络字节序 → 主机字节序)
        uint32_t payload_len;
        std::memcpy(&payload_len, buffer_.data(), sizeof(uint32_t));
        payload_len = ntohl(payload_len);

        // 检查是否收到完整帧 (4 字节长度 + payload_len 字节载荷)
        size_t total_needed = sizeof(uint32_t) + payload_len;
        if (buffer_.size() < total_needed) {
            return false;  // 数据不完整, 等更多数据到达
        }

        // 解析 MessageHeader (从载荷的前 16 字节)
        if (payload_len >= sizeof(MessageHeader)) {
            MessageHeader header;
            std::memcpy(&header, buffer_.data() + sizeof(uint32_t), sizeof(MessageHeader));

            // 计算实际载荷 (除去 MessageHeader 的部分)
            const uint8_t* payload_start = buffer_.data() + sizeof(uint32_t) + sizeof(MessageHeader);
            size_t actual_payload_len = payload_len - sizeof(MessageHeader);

            // 调用用户回调
            if (on_frame) {
                on_frame(header, payload_start, actual_payload_len);
            }
        }

        // 从缓冲区移除已处理的帧数据
        buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<ptrdiff_t>(total_needed));
        return true;  // 成功解码, 继续尝试
    }

    std::vector<uint8_t> buffer_;  // 累积缓冲区 (未处理的数据)
};

} // namespace cyrus
