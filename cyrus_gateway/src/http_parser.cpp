// ============================================================================
// http_parser.cpp - HTTP/1.1 请求解析器实现
// ============================================================================
// 逐字节状态机实现, 解析 HTTP 请求行 → 请求头 → 请求体
// 参考 RFC 7230 (HTTP/1.1 Message Syntax and Routing)
// ============================================================================

#include "cyrus/gateway/http_parser.hpp"

namespace cyrus {
namespace gateway {

// ============================================================================
// parse() - 主入口: 按当前状态分发到对应的处理器
// ============================================================================
size_t HttpParser::parse(const uint8_t* data, size_t len) {
    if (state_ == ParseState::ERROR || state_ == ParseState::COMPLETE) {
        return 0;  // 已经结束, 不处理更多数据
    }

    size_t consumed = 0;

    // 逐字节处理 (简单但正确的策略; 可以优化为逐行)
    // 注意: 每个 handler 通过 consumed 引用参数直接递增消费计数,
    // 返回值仅表示是否取得了进展 (0 = 需要更多数据或出错, >0 = 已处理数据)
    while (consumed < len) {
        // 检查大小限制
        if (total_bytes_parsed_ > MAX_HEADER_SIZE + MAX_BODY_SIZE) {
            set_error("request too large");
            return consumed;
        }

        size_t step_consumed = 0;
        size_t consumed_before_step = consumed;  // 记录此步骤前的消费字节数
        switch (state_) {
            case ParseState::METHOD:
                step_consumed = parse_method(data, len, consumed);
                break;
            case ParseState::PATH:
                step_consumed = parse_path(data, len, consumed);
                break;
            case ParseState::VERSION:
                step_consumed = parse_version(data, len, consumed);
                break;
            case ParseState::HEADER_NAME:
                step_consumed = parse_header_name(data, len, consumed);
                break;
            case ParseState::HEADER_VALUE:
                step_consumed = parse_header_value(data, len, consumed);
                break;
            case ParseState::BODY:
                step_consumed = parse_body(data, len, consumed);
                break;
            case ParseState::CHUNK_SIZE:
                step_consumed = parse_chunk_size(data, len, consumed);
                break;
            case ParseState::CHUNK_DATA:
                step_consumed = parse_chunk_data(data, len, consumed);
                break;
            case ParseState::CHUNK_TRAILER:
                step_consumed = parse_chunk_trailer(data, len, consumed);
                break;
            default:
                return consumed;  // COMPLETE 或 ERROR
        }

        if (step_consumed == 0) {
            // 处理器需要更多数据或出错 (consumed 未变化)
            break;
        }
        // 注意: consumed 已在 handler 内部通过引用递增,
        // 这里仅更新 total_bytes_parsed_ 为当前步骤消耗的实际字节数
        total_bytes_parsed_ += (consumed - consumed_before_step);
    }

    return consumed;
}

// ============================================================================
// parse_method() - 解析 HTTP 方法
// ============================================================================
// 读取 token 字符直到遇到空格, 然后转换方法名 → HttpMethod 枚举
size_t HttpParser::parse_method(const uint8_t* data, size_t len, size_t& consumed) {
    while (consumed < len) {
        uint8_t c = data[consumed];

        if (c == ' ') {
            // 空格 → 方法解析完成
            if (request_.method_str.empty()) {
                set_error("empty HTTP method");
                return 0;
            }
            consumed++;  // 消耗空格
            request_.method = http_method_from_sv(request_.method_str);
            state_ = ParseState::PATH;
            return 1;
        }

        if (!is_token_char(c)) {
            set_error("invalid character in HTTP method");
            return 0;
        }

        request_.method_str += static_cast<char>(c);
        consumed++;

        // 方法名不应过长 (最长 "OPTIONS" = 7 字符, 给 16 字节余量)
        if (request_.method_str.size() > 16) {
            set_error("HTTP method too long");
            return 0;
        }
    }
    return 0;  // 需要更多数据
}

// ============================================================================
// parse_path() - 解析 URI 路径
// ============================================================================
// 读取 URI 直到遇到空格 (\r\n 也会触发错误)
size_t HttpParser::parse_path(const uint8_t* data, size_t len, size_t& consumed) {
    while (consumed < len) {
        uint8_t c = data[consumed];

        if (c == ' ') {
            consumed++;  // 消耗空格
            state_ = ParseState::VERSION;
            return 1;
        }

        // URI 中不应有 \r\n (这表示请求行不完整)
        if (c == '\r' || c == '\n') {
            set_error("unexpected CR/LF in URI");
            return 0;
        }

        request_.uri += static_cast<char>(c);
        consumed++;

        if (request_.uri.size() > MAX_URI_LENGTH) {
            set_error("URI too long");
            return 0;
        }
    }
    return 0;
}

// ============================================================================
// parse_version() - 解析 HTTP 版本
// ============================================================================
// 期望格式: "HTTP/1.1\r\n" 或 "HTTP/1.0\r\n"
size_t HttpParser::parse_version(const uint8_t* data, size_t len, size_t& consumed) {
    while (consumed < len) {
        uint8_t c = data[consumed];
        request_.version += static_cast<char>(c);
        consumed++;

        // 检查 \r\n 终止
        if (c == '\n') {
            // 版本行结束: 检查是否是 "HTTP/x.y"
            if (request_.version.size() < 8 ||
                request_.version.substr(0, 5) != "HTTP/") {
                set_error("invalid HTTP version");
                return 0;
            }
            // 去除尾部的 \r\n
            while (!request_.version.empty() &&
                   (request_.version.back() == '\r' || request_.version.back() == '\n')) {
                request_.version.pop_back();
            }
            state_ = ParseState::HEADER_NAME;
            return 1;
        }

        // 版本行不应太长
        if (request_.version.size() > 16) {
            set_error("HTTP version line too long");
            return 0;
        }
    }
    return 0;
}

// ============================================================================
// parse_header_name() - 解析头部名称
// ============================================================================
// 读取 token 字符直到遇到 ':', 然后转换到 HEADER_VALUE 状态
// 如果遇到 \r\n, 表示头部结束 (空行 → headers_done)
size_t HttpParser::parse_header_name(const uint8_t* data, size_t len, size_t& consumed) {
    while (consumed < len) {
        uint8_t c = data[consumed];

        // 检查是否遇到空行 (\r\n 开头) → 头部区域结束
        if (c == '\r') {
            consumed++;
            if (consumed < len && data[consumed] == '\n') {
                consumed++;
                on_headers_done();
                return 1;
            }
            // 孤立的 \r (非标准但容忍)
            set_error("bare CR in header");
            return 0;
        }

        // 不能以空白开头 (已废弃的 folding, 不支持)
        if (c == ' ' || c == '\t') {
            if (current_header_name_.empty()) {
                set_error("header name cannot start with whitespace");
                return 0;
            }
            // 这可能是过时的行折叠 (已废弃, 忽略)
            consumed++;
            continue;
        }

        if (c == ':') {
            consumed++;  // 消耗 ':'
            state_ = ParseState::HEADER_VALUE;
            return 1;
        }

        if (!is_token_char(c)) {
            set_error("invalid character in header name");
            return 0;
        }

        current_header_name_ += static_cast<char>(c);
        consumed++;

        if (current_header_name_.size() > 256) {
            set_error("header name too long");
            return 0;
        }

        total_header_size_++;
        if (total_header_size_ > MAX_HEADER_SIZE) {
            set_error("headers too large");
            return 0;
        }
    }
    return 0;
}

// ============================================================================
// parse_header_value() - 解析头部值
// ============================================================================
// 读取字符直到遇到 \r\n (头部值结束)
// 跳过前导空白, 去除尾部空白
size_t HttpParser::parse_header_value(const uint8_t* data, size_t len, size_t& consumed) {
    while (consumed < len) {
        uint8_t c = data[consumed];

        if (c == '\r') {
            consumed++;
            if (consumed < len && data[consumed] == '\n') {
                consumed++;
                on_header_complete();  // 处理这个头部
                state_ = ParseState::HEADER_NAME;
                return 1;
            }
            set_error("bare CR in header value");
            return 0;
        }

        // 跳过前导空白
        if (current_header_value_.empty() && (c == ' ' || c == '\t')) {
            consumed++;
            continue;
        }

        current_header_value_ += static_cast<char>(c);
        consumed++;

        total_header_size_++;
        if (total_header_size_ > MAX_HEADER_SIZE) {
            set_error("headers too large");
            return 0;
        }
    }
    return 0;
}

// ============================================================================
// on_header_complete() - 头部解析完成
// ============================================================================
// 去除尾部空白, 存储到 request_.headers
void HttpParser::on_header_complete() {
    // 去除尾部空白
    while (!current_header_value_.empty() &&
           (current_header_value_.back() == ' ' || current_header_value_.back() == '\t')) {
        current_header_value_.pop_back();
    }

    if (!current_header_name_.empty()) {
        if (request_.headers.size() >= MAX_HEADERS) {
            set_error("too many headers");
            return;
        }
        request_.headers.emplace_back(std::move(current_header_name_),
                                       std::move(current_header_value_));
    }

    current_header_name_.clear();
    current_header_value_.clear();
}

// ============================================================================
// on_headers_done() - 所有头部完成
// ============================================================================
// 检查 Content-Length 和 Transfer-Encoding, 确定 Body 解析策略
void HttpParser::on_headers_done() {
    // 检查 Transfer-Encoding: chunked
    auto te = request_.header("Transfer-Encoding");
    if (!te.empty()) {
        // 转小写比较 (简单处理: 包含 "chunked" 即视为 chunked)
        std::transform(te.begin(), te.end(), te.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (te.find("chunked") != std::string::npos) {
            request_.is_chunked = true;
        }
    }

    // 检查 Content-Length
    auto cl = request_.header("Content-Length");
    if (!cl.empty()) {
        try {
            content_length_ = static_cast<size_t>(std::stoull(cl));
            request_.has_content_length = true;
        } catch (...) {
            set_error("invalid Content-Length value");
            return;
        }
    }

    // 检查 Connection 头
    auto conn = request_.header("Connection");
    if (!conn.empty()) {
        std::transform(conn.begin(), conn.end(), conn.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (conn.find("close") != std::string::npos) {
            request_.keep_alive = false;
        }
    }

    // 确定 Body 解析策略
    if (request_.is_chunked) {
        state_ = ParseState::CHUNK_SIZE;
        current_chunk_size_ = 0;
        current_chunk_read_ = 0;
    } else if (request_.has_content_length && content_length_ > 0) {
        state_ = ParseState::BODY;
        body_bytes_read_ = 0;
        request_.body.reserve(content_length_);
    } else {
        // 无请求体: GET 请求等, 直接完成
        finalize_request();
    }
}

// ============================================================================
// parse_body() - 解析固定长度的请求体
// ============================================================================
// 根据 Content-Length 读取精确字节数的请求体。
// 返回实际消耗的字节数 (而非固定 1)。
// 当 body 未读取完整且当前 buffer 已耗尽时, 保持 BODY 状态,
// Connection 层检测到 parser 仍在 BODY 状态时应继续投递 recv。
size_t HttpParser::parse_body(const uint8_t* data, size_t len, size_t& consumed) {
    // 边界情况: Content-Length == 0, 无请求体, 直接完成
    if (content_length_ == 0) {
        request_.body_length = 0;
        finalize_request();
        return 0;  // 未消耗任何字节
    }

    size_t consumed_before = consumed;

    while (consumed < len && body_bytes_read_ < content_length_) {
        if (request_.body.size() > MAX_BODY_SIZE) {
            set_error("request body too large");
            return 0;
        }
        request_.body.push_back(data[consumed]);
        consumed++;
        body_bytes_read_++;
    }

    if (body_bytes_read_ >= content_length_) {
        request_.body_length = body_bytes_read_;
        finalize_request();
    }
    // 返回实际消耗的字节数
    return consumed - consumed_before;
}

// ============================================================================
// parse_chunk_size() - 解析 chunk 大小 (十六进制)
// ============================================================================
// 读取十六进制数字直到 \r\n
// chunk 格式: <hex-size>[;extension]\r\n<data>\r\n
size_t HttpParser::parse_chunk_size(const uint8_t* data, size_t len, size_t& consumed) {
    while (consumed < len) {
        uint8_t c = data[consumed];

        if (c == '\r') {
            consumed++;
            if (consumed < len && data[consumed] == '\n') {
                consumed++;
                // chunk 大小行结束
                if (current_chunk_size_ == 0) {
                    // 终止 chunk (0 大小) → 接下来必须有 trailer-part CRLF
                    // RFC 7230: chunked-body = *chunk last-chunk trailer-part CRLF
                    // trailer-part = *( header-field CRLF )
                    // 关键修复: 不在此处 finalize, 改为过渡到 CHUNK_TRAILER 状态
                    // 以消费 trailer header 行和终止空行 \r\n, 避免残留 \r\n
                    // 进入 pending_data 导致 keep-alive 连接的下一个请求 400 错误
                    state_ = ParseState::CHUNK_TRAILER;
                } else {
                    // 开始接收 chunk 数据
                    state_ = ParseState::CHUNK_DATA;
                    current_chunk_read_ = 0;
                    request_.body.reserve(request_.body.size() + current_chunk_size_);
                }
                return 1;
            }
            set_error("bare CR in chunk size");
            return 0;
        }

        // 跳过 chunk 扩展 (分号后的部分)
        if (c == ';') {
            // 跳过直到 \r\n 的所有字符
            while (consumed < len && data[consumed] != '\r') {
                consumed++;
            }
            continue;
        }

        int hv = hex_value(c);
        if (hv >= 0) {
            current_chunk_size_ = (current_chunk_size_ << 4) | static_cast<size_t>(hv);
            consumed++;
        } else if (c == ' ' || c == '\t') {
            // 忽略十六进制数字后的空白
            consumed++;
        } else {
            set_error("invalid hex digit in chunk size");
            return 0;
        }
    }
    return 0;
}

// ============================================================================
// parse_chunk_data() - 解析 chunk 数据
// ============================================================================
// 读取当前 chunk 的指定字节数, 然后期望 \r\n
size_t HttpParser::parse_chunk_data(const uint8_t* data, size_t len, size_t& consumed) {
    while (consumed < len && current_chunk_read_ < current_chunk_size_) {
        request_.body.push_back(data[consumed]);
        consumed++;
        current_chunk_read_++;

        if (request_.body.size() > MAX_BODY_SIZE) {
            set_error("request body too large");
            return 0;
        }
    }

    if (current_chunk_read_ >= current_chunk_size_) {
        // 当前 chunk 数据接收完毕, 消耗尾部 \r\n
        if (consumed < len) {
            if (data[consumed] == '\r') {
                consumed++;
                if (consumed < len && data[consumed] == '\n') {
                    consumed++;
                } else {
                    set_error("expected LF after CR in chunk end");
                    return 0;
                }
            } else {
                set_error("expected CRLF after chunk data");
                return 0;
            }
        } else {
            // 需要更多数据来读取尾部的 \r\n
            return 0;
        }

        // 准备下一个 chunk
        current_chunk_size_ = 0;
        state_ = ParseState::CHUNK_SIZE;
    }
    return 1;
}

// ============================================================================
// parse_chunk_trailer() - 消费 chunked trailer-part CRLF
// ============================================================================
// RFC 7230: trailer-part = *( header-field CRLF )
// 在最后一个 chunk (0\r\n) 之后, 必须有一个 trailer-part 和 CRLF.
// trailer-part 可以为空 (直接 \r\n) 或包含 trailer header 行.
// 此函数跳过所有 trailer header 行, 直到遇到空行 (\r\n),
// 然后调用 finalize_request() 完成解析.
// 关键修复: 确保终止 \r\n 被消费, 不残留到 pending_data 导致后续请求错位.
size_t HttpParser::parse_chunk_trailer(const uint8_t* data, size_t len, size_t& consumed) {
    size_t consumed_before = consumed;
    size_t line_start = consumed;  // 当前行起始位置, 用于检测空行

    while (consumed < len) {
        if (data[consumed] == '\r') {
            if (consumed + 1 < len && data[consumed + 1] == '\n') {
                consumed += 2;  // 消费 \r\n
                if (consumed - line_start == 2) {
                    // 空行 (\r\n 独立一行) → trailer 结束
                    finalize_request();
                    return consumed - consumed_before;
                }
                // 非空行 (trailer header), 继续扫描下一行
                line_start = consumed;
                continue;
            }
            // \r 在 buffer 末尾, 无法判断是否是 \r\n, 需要更多数据
            return consumed - consumed_before;
        }
        // 跳过 trailer header 行中的字符 (header-name, ':', header-value 等)
        consumed++;
    }
    return consumed - consumed_before;
}

// ============================================================================
// finalize_request() - 请求解析完成
// ============================================================================
void HttpParser::finalize_request() {
    request_.body_length = request_.body.size();

    // HTTP/1.0 默认不 keep-alive (除非显式声明)
    if (request_.version == "HTTP/1.0") {
        auto conn = request_.header("Connection");
        if (conn.empty()) {
            request_.keep_alive = false;
        }
    }

    state_ = ParseState::COMPLETE;
    LOG_DEBUG("HTTP request parsed: {} {} {} (body: {} bytes, keep-alive: {})",
              http_method_to_sv(request_.method), request_.uri, request_.version,
              request_.body_length, request_.keep_alive);
}

// ============================================================================
// reset() - 重置解析器
// ============================================================================
void HttpParser::reset() {
    state_ = ParseState::METHOD;
    request_.reset();
    current_header_name_.clear();
    current_header_value_.clear();
    body_bytes_read_ = 0;
    content_length_ = 0;
    current_chunk_size_ = 0;
    current_chunk_read_ = 0;
    total_bytes_parsed_ = 0;
    total_header_size_ = 0;
    last_char_ = 0;
    has_last_char_ = false;
}

// ============================================================================
// state_name() - 获取状态名称 (调试)
// ============================================================================
const char* HttpParser::state_name(ParseState s) {
    switch (s) {
        case ParseState::METHOD:       return "METHOD";
        case ParseState::PATH:         return "PATH";
        case ParseState::VERSION:      return "VERSION";
        case ParseState::HEADER_NAME:  return "HEADER_NAME";
        case ParseState::HEADER_VALUE: return "HEADER_VALUE";
        case ParseState::BODY:         return "BODY";
        case ParseState::CHUNK_SIZE:   return "CHUNK_SIZE";
        case ParseState::CHUNK_DATA:   return "CHUNK_DATA";
        case ParseState::CHUNK_TRAILER:return "CHUNK_TRAILER";
        case ParseState::COMPLETE:     return "COMPLETE";
        case ParseState::ERROR:        return "ERROR";
        default:                       return "UNKNOWN";
    }
}

} // namespace gateway
} // namespace cyrus
