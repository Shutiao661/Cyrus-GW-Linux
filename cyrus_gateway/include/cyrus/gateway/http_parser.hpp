// ============================================================================
// http_parser.hpp - HTTP/1.1 请求解析器 (状态机模式)
// ============================================================================
// 逐字节解析 HTTP/1.1 请求, 使用有限状态机 (FSM) 设计:
//
//   状态转换图:
//   ┌─────────┐  空格   ┌──────┐  空格   ┌─────────┐  \r\n  ┌─────────────┐
//   │ METHOD  │ ──────→ │ PATH │ ──────→ │ VERSION │ ─────→ │ HEADER_NAME │
//   └─────────┘         └──────┘         └─────────┘        └─────────────┘
//                                                              │     ↑
//                                                              │ :   │ \r\n
//                                                              ↓     │
//                                                         ┌────────────┐
//                                                         │HEADER_VALUE│
//                                                         └────────────┘
//                                                              │
//                                              空行(\r\n\r\n) │
//                                                              ↓
//                                           ┌──────────┐  读完  ┌──────────┐
//                                           │   BODY   │ ─────→ │ COMPLETE │
//                                           └──────────┘        └──────────┘
//
// 支持特性:
//   - HTTP/1.0 和 HTTP/1.1
//   - Content-Length (精确字节数)
//   - Transfer-Encoding: chunked (分块传输)
//   - Connection: keep-alive / close
//   - 请求头解析 (名称和值)
//   - 错误检测 (格式错误 → ERROR 状态)
// ============================================================================

#pragma once

#include "cyrus/types.hpp"
#include "cyrus/logger.hpp"

#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cctype>

namespace cyrus {
namespace gateway {

// ============================================================================
// ParseState - 解析器状态枚举
// ============================================================================
enum class ParseState : uint8_t {
    METHOD,          // 正在解析 HTTP 方法 (GET/POST/...)
    PATH,            // 正在解析 URI 路径
    VERSION,         // 正在解析 HTTP 版本 (HTTP/1.x)
    HEADER_NAME,     // 正在解析头部名称
    HEADER_VALUE,    // 正在解析头部值
    BODY,            // 正在接收请求体 (根据 Content-Length 或 chunked)
    CHUNK_SIZE,      // 正在解析 chunk 大小 (十六进制)
    CHUNK_DATA,      // 正在接收 chunk 数据
    CHUNK_TRAILER,   // 正在解析 chunk 尾部 (跳过)
    COMPLETE,        // 请求解析完成
    ERROR            // 解析错误 (格式错误)
};

// ============================================================================
// ParsedRequest - 解析完成的 HTTP 请求
// ============================================================================
struct ParsedRequest {
    HttpMethod method = HttpMethod::UNKNOWN;   // HTTP 方法
    std::string method_str;                     // 原始方法字符串
    std::string uri;                            // 请求 URI (路径 + 查询字符串)
    std::string version;                        // HTTP 版本字符串 (如 "HTTP/1.1")

    // 请求头 (保留顺序 + 支持快速查找)
    std::vector<std::pair<std::string, std::string>> headers;

    // 请求体
    std::vector<uint8_t> body;                  // 请求体数据
    size_t body_length = 0;                     // 请求体长度

    // HTTP 语义标志
    bool keep_alive = true;                     // Connection: keep-alive?
    bool has_content_length = false;            // 有 Content-Length 头?
    bool is_chunked = false;                    // Transfer-Encoding: chunked?

    // --- 辅助方法 ---

    // 通过名称查找头部值 (不区分大小写)
    std::string header(const std::string& name) const {
        for (const auto& [h_name, h_value] : headers) {
            if (iequals(h_name, name)) {
                return h_value;
            }
        }
        return "";
    }

    // 获取 Content-Length 数值
    size_t content_length() const {
        auto val = header("Content-Length");
        if (val.empty()) return 0;
        try {
            return static_cast<size_t>(std::stoull(val));
        } catch (...) {
            return 0;
        }
    }

    // 重置所有字段 (用于连接复用)
    void reset() {
        method = HttpMethod::UNKNOWN;
        method_str.clear();
        uri.clear();
        version.clear();
        headers.clear();
        body.clear();
        body_length = 0;
        keep_alive = true;
        has_content_length = false;
        is_chunked = false;
    }

private:
    static bool iequals(const std::string& a, const std::string& b) {
        if (a.size() != b.size()) return false;
        return std::equal(a.begin(), a.end(), b.begin(),
            [](char c1, char c2) {
                return std::tolower(static_cast<unsigned char>(c1))
                    == std::tolower(static_cast<unsigned char>(c2));
            });
    }
};

// ============================================================================
// HttpParser - HTTP 请求解析器
// ============================================================================
class HttpParser {
public:
    // --- 配置常量 ---
    static constexpr size_t MAX_HEADER_SIZE   = 8192;     // 最大头部总大小 (8KB)
    static constexpr size_t MAX_URI_LENGTH    = 2048;     // 最大 URI 长度
    static constexpr size_t MAX_HEADERS       = 64;       // 最大头部数量
    static constexpr size_t MAX_BODY_SIZE     = 1048576;  // 最大请求体大小 (1MB)

    // --- 返回值: 消耗的字节数 ---
    // parse() 返回成功解析的字节数。
    // 返回 0 且 state() == ERROR 表示解析错误。
    // 返回 0 且 state() 不是 ERROR 表示需要更多数据。
    // 返回 > 0 时调用者应从缓冲区移除对应字节。

    // 喂入数据块进行解析
    // data: 新到达的数据
    // len: 数据长度
    // 返回: 消耗的字节数 (0 表示需要更多数据或出错)
    size_t parse(const uint8_t* data, size_t len);

    // 当前解析状态
    ParseState state() const noexcept { return state_; }

    // 解析是否完成?
    bool is_complete() const noexcept { return state_ == ParseState::COMPLETE; }

    // 解析是否出错?
    bool is_error() const noexcept { return state_ == ParseState::ERROR; }

    // 获取解析结果 (仅在 is_complete() 时有效)
    const ParsedRequest& result() const noexcept { return request_; }

    // 重置解析器 (用于下一个请求, 如 keep-alive)
    void reset();

    // 调试: 获取状态名
    static const char* state_name(ParseState s);

private:
    // --- 解析辅助 ---

    // 判断字符是否是 token 字符 (RFC 7230 Section 3.2.6)
    // token = 1*tchar
    // tchar = "!" / "#" / "$" / "%" / "&" / "'" / "*" / "+" / "-" / "." /
    //         "^" / "_" / "`" / "|" / "~" / DIGIT / ALPHA
    static bool is_token_char(uint8_t c) {
        if (c >= 'a' && c <= 'z') return true;
        if (c >= 'A' && c <= 'Z') return true;
        if (c >= '0' && c <= '9') return true;
        switch (c) {
            case '!': case '#': case '$': case '%': case '&': case '\'':
            case '*': case '+': case '-': case '.': case '^': case '_':
            case '`': case '|': case '~': return true;
            default: return false;
        }
    }

    // 判断是否是空白字符 (RFC 7230: SP / HTAB)
    static bool is_whitespace(uint8_t c) {
        return c == ' ' || c == '\t';
    }

    // 十六进制数字值 (0-15), 无效返回 -1
    static int hex_value(uint8_t c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    }

    // --- 解析状态处理器 ---
    size_t parse_method(const uint8_t* data, size_t len, size_t& consumed);
    size_t parse_path(const uint8_t* data, size_t len, size_t& consumed);
    size_t parse_version(const uint8_t* data, size_t len, size_t& consumed);
    size_t parse_header_name(const uint8_t* data, size_t len, size_t& consumed);
    size_t parse_header_value(const uint8_t* data, size_t len, size_t& consumed);
    size_t parse_body(const uint8_t* data, size_t len, size_t& consumed);
    size_t parse_chunk_size(const uint8_t* data, size_t len, size_t& consumed);
    size_t parse_chunk_data(const uint8_t* data, size_t len, size_t& consumed);
    size_t parse_chunk_trailer(const uint8_t* data, size_t len, size_t& consumed);

    // --- 头部处理 ---
    // 当一个头部解析完成后调用 (头部名: 头部值)
    void on_header_complete();

    // 当所有头部解析完成后调用 (空行到达)
    void on_headers_done();

    // 请求解析完成
    void finalize_request();

    ParseState state_ = ParseState::METHOD;
    ParsedRequest request_;

    // 当前正在解析的头部名/值
    std::string current_header_name_;
    std::string current_header_value_;

    // Body 解析状态
    size_t body_bytes_read_ = 0;        // 已读取的请求体字节数
    size_t content_length_ = 0;         // Content-Length 值

    // Chunked 状态
    size_t current_chunk_size_ = 0;     // 当前 chunk 的大小 (字节)
    size_t current_chunk_read_ = 0;     // 当前 chunk 中已读取的字节

    // 保持上次未完成的字符 (用于 \r\n 处理)
    uint8_t last_char_ = 0;
    bool has_last_char_ = false;

    // 已解析的总字节数 (用于防止过大请求)
    size_t total_bytes_parsed_ = 0;
    size_t total_header_size_ = 0;      // 头部区域总大小

    // 设置错误状态
    void set_error(const char* reason) {
        LOG_WARN("HTTP parse error: {} (state={})", reason, state_name(state_));
        state_ = ParseState::ERROR;
    }
};

} // namespace gateway
} // namespace cyrus
