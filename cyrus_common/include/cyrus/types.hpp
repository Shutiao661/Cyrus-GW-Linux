// ============================================================================
// types.hpp - Cyrus-GW 核心类型定义
// ============================================================================
// 定义整个项目中使用的共享类型:
//   - ErrorCode 枚举: 统一的错误码体系, 按模块分段
//   - HttpMethod 枚举: HTTP 请求方法 (GET, POST, PUT, DELETE ...)
//   - HttpStatus 枚举: HTTP 响应状态码 (200, 400, 404, 500 ...)
//   - LogLevel 枚举: 日志级别 (DEBUG, INFO, WARN, ERROR, FATAL)
//   - 工具函数: 枚举值 → 字符串转换
// ============================================================================

#pragma once

#include "platform.hpp"

namespace cyrus {

// ============================================================================
// 错误码体系 (ErrorCode)
// ============================================================================
// 错误码按来源分段, 每段 100 个值, 便于快速定位问题来源
// - 1xx: 网络层错误
// - 2xx: HTTP 协议层错误
// - 3xx: I/O 引擎错误
// - 4xx: Agent 通信错误
// - 5xx: 内部错误
// ============================================================================
enum class ErrorCode : uint16_t {
    // --- 成功 ---
    OK = 0,

    // --- 网络错误 (100-199) ---
    E_CONNECTION_RESET   = 100,   // 连接被对端重置 (客户端断开或网络中断)
    E_CONNECTION_TIMEOUT = 101,   // 连接超时 (TCP 握手 / 读写超时)
    E_CONNECTION_REFUSED = 102,   // 连接被拒绝 (目标端口未监听)
    E_ADDRESS_IN_USE     = 103,   // 地址已被占用 (bind 失败, 端口冲突)
    E_NETWORK_UNREACHABLE = 104,  // 网络不可达

    // --- HTTP 协议错误 (200-299) ---
    E_HTTP_PARSE_ERROR      = 200,  // HTTP 请求格式错误 (无法解析)
    E_HTTP_BODY_TRUNCATED   = 201,  // HTTP 请求体不完整 (Content-Length 与实际不符)
    E_HTTP_HEADER_TOO_LARGE = 202,  // HTTP 请求头超过大小限制
    E_HTTP_METHOD_INVALID   = 203,  // 不支持的 HTTP 方法
    E_HTTP_URI_TOO_LONG     = 204,  // URI 超过最大长度限制
    E_HTTP_VERSION_INVALID  = 205,  // HTTP 版本不支持 (不是 HTTP/1.0 或 HTTP/1.1)
    E_SSE_PARSE_ERROR       = 210,  // SSE (Server-Sent Events) 解析错误

    // --- I/O 引擎错误 (300-399) ---
    E_ENGINE_INIT_FAILED   = 300,   // I/O 引擎初始化失败 (如 io_uring 队列创建失败)
    E_ENGINE_OP_POST_FAILED = 301,  // 异步操作投递失败 (io_uring 提交返回错误)
    E_ENGINE_SHUTDOWN      = 302,   // I/O 引擎正在关闭

    // --- Agent 通信错误 (400-499) ---
    E_AGENT_UNREACHABLE    = 400,   // Agent 不可达 (TCP 连接失败)
    E_AGENT_PROTOCOL_ERROR = 401,   // Agent 协议错误 (收到的数据无法解码)
    E_AGENT_TIMEOUT        = 402,   // Agent 响应超时
    E_AGENT_INTERNAL_ERROR = 403,   // Agent 内部错误 (Agent 返回错误响应)

    // --- 内部错误 (500-599) ---
    E_INTERNAL             = 500,   // 未分类内部错误
    E_NO_BUFFERS           = 501,   // 缓冲池耗尽 (高负载时需要增大池大小)
    E_INVALID_STATE        = 502,   // 非法的状态转换 (连接状态机逻辑错误)
};

// ============================================================================
// HTTP 相关枚举
// ============================================================================

// HTTP 请求方法
enum class HttpMethod : uint8_t {
    GET     = 0,
    POST    = 1,
    PUT     = 2,
    DELETE  = 3,
    PATCH   = 4,
    HEAD    = 5,
    OPTIONS = 6,
    UNKNOWN = 255  // 无法识别的方法
};

// HTTP 方法 → 字符串
inline constexpr std::string_view http_method_to_sv(HttpMethod method) {
    switch (method) {
        case HttpMethod::GET:     return "GET";
        case HttpMethod::POST:    return "POST";
        case HttpMethod::PUT:     return "PUT";
        case HttpMethod::DELETE:  return "DELETE";
        case HttpMethod::PATCH:   return "PATCH";
        case HttpMethod::HEAD:    return "HEAD";
        case HttpMethod::OPTIONS: return "OPTIONS";
        default:                  return "UNKNOWN";
    }
}

// 字符串 → HTTP 方法 (大小写敏感, 期望输入为大写)
inline constexpr HttpMethod http_method_from_sv(std::string_view sv) {
    if (sv == "GET")     return HttpMethod::GET;
    if (sv == "POST")    return HttpMethod::POST;
    if (sv == "PUT")     return HttpMethod::PUT;
    if (sv == "DELETE")  return HttpMethod::DELETE;
    if (sv == "PATCH")   return HttpMethod::PATCH;
    if (sv == "HEAD")    return HttpMethod::HEAD;
    if (sv == "OPTIONS") return HttpMethod::OPTIONS;
    return HttpMethod::UNKNOWN;
}

// HTTP 响应状态码 (仅列出常用值, 完整列表参考 RFC 7231)
enum class HttpStatus : uint16_t {
    OK                    = 200,   // 请求成功
    CREATED               = 201,   // 资源已创建
    NO_CONTENT            = 204,   // 无内容 (成功但无响应体)
    BAD_REQUEST           = 400,   // 请求格式错误 (语法错误)
    NOT_FOUND             = 404,   // 资源未找到
    METHOD_NOT_ALLOWED    = 405,   // 不支持的 HTTP 方法
    REQUEST_TIMEOUT       = 408,   // 请求超时
    PAYLOAD_TOO_LARGE     = 413,   // 请求体过大
    URI_TOO_LONG          = 414,   // URI 过长
    INTERNAL_SERVER_ERROR = 500,   // 服务器内部错误
    NOT_IMPLEMENTED       = 501,   // 功能未实现
    BAD_GATEWAY           = 502,   // 上游服务返回无效响应
    SERVICE_UNAVAILABLE   = 503,   // 服务暂时不可用 (过载/维护)
    GATEWAY_TIMEOUT       = 504,   // 上游服务响应超时
};

// HTTP 状态码 → 原因短语 (Reason Phrase, RFC 7231)
inline const char* http_status_text(HttpStatus status) {
    switch (status) {
        case HttpStatus::OK:                    return "OK";
        case HttpStatus::CREATED:               return "Created";
        case HttpStatus::NO_CONTENT:            return "No Content";
        case HttpStatus::BAD_REQUEST:           return "Bad Request";
        case HttpStatus::NOT_FOUND:             return "Not Found";
        case HttpStatus::METHOD_NOT_ALLOWED:    return "Method Not Allowed";
        case HttpStatus::REQUEST_TIMEOUT:       return "Request Timeout";
        case HttpStatus::PAYLOAD_TOO_LARGE:     return "Payload Too Large";
        case HttpStatus::URI_TOO_LONG:          return "URI Too Long";
        case HttpStatus::INTERNAL_SERVER_ERROR: return "Internal Server Error";
        case HttpStatus::NOT_IMPLEMENTED:       return "Not Implemented";
        case HttpStatus::BAD_GATEWAY:           return "Bad Gateway";
        case HttpStatus::SERVICE_UNAVAILABLE:   return "Service Unavailable";
        case HttpStatus::GATEWAY_TIMEOUT:       return "Gateway Timeout";
        default:                                return "Unknown";
    }
}

// ============================================================================
// 日志相关枚举
// ============================================================================

// 日志级别 (从低到高, 数值越大越严重)
enum class LogLevel : uint8_t {
    DEBUG = 0,   // 调试信息 (开发环境使用, 生产环境关闭)
    INFO  = 1,   // 常规信息 (启动/关闭/配置等)
    WARN  = 2,   // 警告 (非预期但不影响运行的情况)
    ERROR = 3,   // 错误 (需要关注, 但进程继续运行)
    FATAL = 4,   // 致命错误 (进程即将退出)
};

inline const char* log_level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO";
        case LogLevel::WARN:  return "WARN";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
        default:              return "????";
    }
}

} // namespace cyrus
