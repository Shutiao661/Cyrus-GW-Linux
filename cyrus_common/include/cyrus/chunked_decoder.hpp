// ============================================================================
// chunked_decoder.hpp - HTTP Chunked Transfer-Encoding 解码器
// ============================================================================
// 从 chunked 编码的字节流中剥离 chunk 长度行和尾部 CRLF,
// 输出纯净的原始数据。用于解码 Agent 返回的 chunked HTTP 响应。
//
// Chunked 编码格式 (RFC 7230 Section 4.1):
//   <hex-size>[;extension]\r\n
//   <data>\r\n
//   ...
//   0\r\n
//   <trailer>\r\n
//
// 问题: 如果 Agent 返回的 SSE 流使用 chunked 编码, chunk 长度行
// (如 "1a4\r\n") 会泄漏到 SSE 数据中, 破坏 SSE 协议解析。
// ChunkedDecoder 在解析层剥离所有传输编码, 输出纯净的 SSE 文本流。
//
// 使用方式:
//   ChunkedDecoder decoder;
//   decoder.on_data = [](const uint8_t* data, size_t len) {
//       // 处理去 chunk 化的纯净数据
//   };
//   decoder.feed(recv_buffer, bytes_recv);
// ============================================================================

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <functional>
#include <algorithm>
#include <cctype>
#include <optional>

namespace cyrus {

// ============================================================================
// ChunkedDecoder - HTTP Chunked 传输解码器
// ============================================================================
class ChunkedDecoder {
public:
    // 解码状态
    enum class State : uint8_t {
        READ_SIZE,    // 正在读取 chunk 大小 (十六进制行)
        READ_DATA,    // 正在读取 chunk 数据
        READ_TRAILER, // 正在读取尾部 CRLF (chunk 数据后的)
        DONE,         // 解码完成 (遇到 0 大小 chunk)
        ERROR         // 解码错误
    };

    // 数据回调: (数据指针, 数据长度)
    // 仅在 READ_DATA 状态输出, 输出的是去 chunk 化的纯净数据
    using DataCallback = std::function<void(const uint8_t* data, size_t len)>;
    DataCallback on_data;

    // --- 配置 ---
    static constexpr size_t MAX_CHUNK_SIZE = 10 * 1024 * 1024;  // 单个 chunk 最大 10MB

    // --- 喂入数据 ---
    // 内部状态机循环解析, 每解析出一个 chunk 数据块即触发 on_data 回调
    void feed(const uint8_t* data, size_t len) {
        buffer_.insert(buffer_.end(), data, data + len);
        process_buffer();
    }

    // --- 状态查询 ---
    State state() const noexcept { return state_; }
    bool is_done() const noexcept { return state_ == State::DONE; }
    bool is_error() const noexcept { return state_ == State::ERROR; }
    const char* error_message() const noexcept { return error_msg_; }

    // --- 重置 ---
    void reset() {
        state_ = State::READ_SIZE;
        buffer_.clear();
        current_chunk_size_ = 0;
        current_chunk_read_ = 0;
        error_msg_ = nullptr;
    }

private:
    void process_buffer() {
        bool progress = true;
        while (buffer_.size() > 0 && state_ != State::DONE && state_ != State::ERROR && progress) {
            progress = false;
            size_t before = buffer_.size();
            switch (state_) {
                case State::READ_SIZE:
                    process_read_size();
                    break;
                case State::READ_DATA:
                    process_read_data();
                    break;
                case State::READ_TRAILER:
                    process_read_trailer();
                    break;
                default:
                    return;
            }
            // 检测是否消耗了数据 (防止在等待更多数据时死循环)
            if (buffer_.size() < before) progress = true;
        }
    }

    // --- READ_SIZE: 解析 chunk 大小行 ---
    void process_read_size() {
        // 查找 \r\n
        auto crlf = find_crlf();
        if (!crlf) return;  // 行不完整, 等待更多数据

        size_t line_start = 0;
        size_t line_end = crlf->first;  // \r 的位置

        // 解析十六进制大小
        current_chunk_size_ = 0;
        for (size_t i = line_start; i < line_end; ++i) {
            uint8_t c = buffer_[i];
            if (c == ';') break;  // chunk 扩展, 停止解析
            int hv = hex_value(c);
            if (hv < 0) {
                // 忽略空白 (chunk 扩展前的空格)
                if (c == ' ' || c == '\t') continue;
                set_error("invalid hex digit in chunk size");
                return;
            }
            current_chunk_size_ = (current_chunk_size_ << 4) | static_cast<size_t>(hv);
            if (current_chunk_size_ > MAX_CHUNK_SIZE) {
                set_error("chunk size exceeds maximum");
                return;
            }
        }

        // 消耗整行 (包括 \r\n)
        size_t consume_len = crlf->second + 1;  // \n 之后的位置
        buffer_.erase(buffer_.begin(), buffer_.begin() + consume_len);

        if (current_chunk_size_ == 0) {
            // 终止 chunk, 进入 DONE
            state_ = State::DONE;
        } else {
            state_ = State::READ_DATA;
            current_chunk_read_ = 0;
        }
    }

    // --- READ_DATA: 读取 chunk 数据 ---
    void process_read_data() {
        size_t remaining = current_chunk_size_ - current_chunk_read_;
        size_t available = buffer_.size();

        if (available == 0) return;  // 等待更多数据

        size_t to_output = (available < remaining) ? available : remaining;

        // 输出去 chunk 化的纯净数据
        if (on_data && to_output > 0) {
            on_data(buffer_.data(), to_output);
        }

        current_chunk_read_ += to_output;
        buffer_.erase(buffer_.begin(), buffer_.begin() + to_output);

        if (current_chunk_read_ >= current_chunk_size_) {
            // 当前 chunk 数据读取完毕, 接下来期望 CRLF
            state_ = State::READ_TRAILER;
        }
    }

    // --- READ_TRAILER: 消耗 chunk 数据后的 CRLF ---
    void process_read_trailer() {
        if (buffer_.size() < 2) return;  // 等待更多数据

        if (buffer_[0] == '\r' && buffer_[1] == '\n') {
            buffer_.erase(buffer_.begin(), buffer_.begin() + 2);
            state_ = State::READ_SIZE;  // 准备读取下一个 chunk
        } else {
            // 宽松处理: 容忍缺失的 CRLF (某些实现可能不发送)
            state_ = State::READ_SIZE;
        }
    }

    // --- 辅助: 查找缓冲区中的 \r\n ---
    std::optional<std::pair<size_t, size_t>> find_crlf() const {
        for (size_t i = 0; i + 1 < buffer_.size(); ++i) {
            if (buffer_[i] == '\r' && buffer_[i + 1] == '\n') {
                return std::make_pair(i, i + 1);
            }
        }
        return std::nullopt;
    }

    // --- 十六进制数字值 ---
    static int hex_value(uint8_t c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    }

    void set_error(const char* msg) {
        state_  = State::ERROR;
        error_msg_ = msg;
    }

    State state_ = State::READ_SIZE;
    std::vector<uint8_t> buffer_;
    size_t current_chunk_size_ = 0;
    size_t current_chunk_read_ = 0;
    const char* error_msg_ = nullptr;
};

} // namespace cyrus
