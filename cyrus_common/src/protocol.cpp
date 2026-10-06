// ============================================================================
// protocol.cpp - 二进制协议编解码实现
// ============================================================================
// 实现 ProtocolCodec 的编码方法和 ProtocolDecoder 的流式解码。
//
// 协议帧格式 (简化版):
//   [u32 payload_len (网络字节序)] [MessageHeader (16B)] [payload bytes...]
//
// MessageHeader 结构:
//   [u32 total_len][u64 request_id][u8 msg_type][u8 flags][u16 reserved]
//   total_len = sizeof(MessageHeader) + 实际 payload 长度
// ============================================================================

#include "cyrus/protocol.hpp"
#include "cyrus/logger.hpp"

#include <cstring>
#include <vector>
#include <string_view>

namespace cyrus {

// ============================================================================
// 内部辅助: 将单个值以网络字节序附加到字节向量
// ============================================================================
namespace {

// 写入 uint32_t (大端)
void write_u32(std::vector<uint8_t>& buf, uint32_t val) {
    uint32_t net_val = htonl(val);
    const auto* bytes = reinterpret_cast<const uint8_t*>(&net_val);
    buf.insert(buf.end(), bytes, bytes + sizeof(uint32_t));
}

// 写入 uint16_t (大端)
void write_u16(std::vector<uint8_t>& buf, uint16_t val) {
    uint16_t net_val = htons(val);
    const auto* bytes = reinterpret_cast<const uint8_t*>(&net_val);
    buf.insert(buf.end(), bytes, bytes + sizeof(uint16_t));
}

// 写入字符串 (长度前缀格式: u16 length + data)
void write_string(std::vector<uint8_t>& buf, std::string_view sv) {
    uint16_t len = static_cast<uint16_t>(sv.size());
    write_u16(buf, len);
    buf.insert(buf.end(), sv.begin(), sv.end());
}

} // anonymous namespace

// ============================================================================
// ProtocolCodec 公共编码方法
// ============================================================================

// --- 编码客户端请求 ---
// 载荷格式:
//   [method_str][uri_str][header_count:u16][h1_name][h1_value]...[body]
std::vector<uint8_t> ProtocolCodec::encode_request(
    uint64_t request_id,
    std::string_view method,
    std::string_view uri,
    const std::vector<std::pair<std::string, std::string>>& headers,
    std::string_view body)
{
    std::vector<uint8_t> payload;

    // --- 构建 MessageHeader ---
    MessageHeader header;
    header.request_id = request_id;
    header.msg_type   = MessageType::MSG_REQUEST;
    header.flags      = 0;

    // --- 构建载荷 (序列化格式) ---
    write_string(payload, method);   // HTTP 方法
    write_string(payload, uri);      // 请求 URI
    write_u16(payload, static_cast<uint16_t>(headers.size()));  // Header 数量
    for (const auto& [name, value] : headers) {
        write_string(payload, name);
        write_string(payload, value);
    }
    write_string(payload, body);     // 请求体

    // 设置 total_len
    header.total_len = static_cast<uint32_t>(sizeof(MessageHeader) + payload.size());

    return build_frame(header, payload.data(), payload.size());
}

// --- 编码响应头 ---
// 载荷格式: [status:u16][header_count:u16][h1_name][h1_value]...
std::vector<uint8_t> ProtocolCodec::encode_response_headers(
    uint64_t request_id,
    uint16_t status,
    const std::vector<std::pair<std::string, std::string>>& headers)
{
    std::vector<uint8_t> payload;

    MessageHeader header;
    header.request_id = request_id;
    header.msg_type   = MessageType::MSG_RESPONSE_HEADERS;
    header.flags      = 0;

    write_u16(payload, status);
    write_u16(payload, static_cast<uint16_t>(headers.size()));
    for (const auto& [name, value] : headers) {
        write_string(payload, name);
        write_string(payload, value);
    }

    header.total_len = static_cast<uint32_t>(sizeof(MessageHeader) + payload.size());
    return build_frame(header, payload.data(), payload.size());
}

// --- 编码响应数据块 ---
// 载荷格式: [data_bytes...]
std::vector<uint8_t> ProtocolCodec::encode_response_data(
    uint64_t request_id,
    std::string_view data,
    bool is_last)
{
    std::vector<uint8_t> payload;

    MessageHeader header;
    header.request_id = request_id;
    header.msg_type   = MessageType::MSG_RESPONSE_DATA;
    header.flags      = is_last ? FrameFlags::FLAG_END_OF_STREAM : 0;

    // 直接将数据作为载荷
    payload.insert(payload.end(), data.begin(), data.end());

    header.total_len = static_cast<uint32_t>(sizeof(MessageHeader) + payload.size());
    return build_frame(header, payload.data(), payload.size());
}

// --- 编码响应结束 ---
std::vector<uint8_t> ProtocolCodec::encode_response_end(uint64_t request_id) {
    std::vector<uint8_t> payload;

    MessageHeader header;
    header.request_id = request_id;
    header.msg_type   = MessageType::MSG_RESPONSE_END;
    header.flags      = FrameFlags::FLAG_END_OF_STREAM;
    header.total_len  = static_cast<uint32_t>(sizeof(MessageHeader));

    return build_frame(header, nullptr, 0);
}

// --- 编码错误响应 ---
// 载荷格式: [error_code:u16][error_message_str]
std::vector<uint8_t> ProtocolCodec::encode_error(
    uint64_t request_id,
    ErrorCode error_code,
    std::string_view error_message)
{
    std::vector<uint8_t> payload;

    MessageHeader header;
    header.request_id = request_id;
    header.msg_type   = MessageType::MSG_ERROR;
    header.flags      = FrameFlags::FLAG_ERROR;

    write_u16(payload, static_cast<uint16_t>(error_code));
    write_string(payload, error_message);

    header.total_len = static_cast<uint32_t>(sizeof(MessageHeader) + payload.size());
    return build_frame(header, payload.data(), payload.size());
}

// ============================================================================
// 内部: 构建完整帧
// ============================================================================
// 帧格式: [u32 payload_len (NET)] [payload_bytes...]
// NOTE: payload_len 不包括自身的 4 字节, 仅表示后面的 payload 长度
std::vector<uint8_t> ProtocolCodec::build_frame(
    const MessageHeader& header,
    const uint8_t* payload,
    size_t payload_len)
{
    // 序列化 MessageHeader → 16 字节
    uint8_t header_bytes[sizeof(MessageHeader)];
    std::memcpy(header_bytes, &header, sizeof(MessageHeader));

    // 完整载荷 = MessageHeader (16B) + 实际 payload
    size_t total_payload_len = sizeof(MessageHeader) + payload_len;

    // 构建帧: 4 字节长度前缀 + 完整载荷
    std::vector<uint8_t> frame;
    frame.reserve(sizeof(uint32_t) + total_payload_len);

    // 写入长度前缀 (网络字节序)
    write_u32(frame, static_cast<uint32_t>(total_payload_len));

    // 写入 MessageHeader
    frame.insert(frame.end(), header_bytes, header_bytes + sizeof(MessageHeader));

    // 写入实际载荷
    if (payload && payload_len > 0) {
        frame.insert(frame.end(), payload, payload + payload_len);
    }

    return frame;
}

} // namespace cyrus
