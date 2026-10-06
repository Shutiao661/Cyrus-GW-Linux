// ============================================================================
// echo_handler.hpp - Echo 请求处理器
// ============================================================================
// 将所有请求体回显给客户端, 用于调试和连通性测试。
//
// 请求: POST /echo { ... any JSON ... }
// 响应: { "echo": true, "request_body": ..., "body_length": N }
// ============================================================================

#pragma once

#include "request_handler.hpp"
#include "cyrus/sse_codec.hpp"

#include <format>

namespace cyrus {
namespace agent {

class EchoHandler : public RequestHandler {
public:
    AgentResponse handle(const AgentRequest& req) override {
        // 构建回显响应 JSON
        std::string json = std::format(
            R"({{"echo":true,"method":"{}","uri":"{}","body_length":{},"received_body":"{}"}})",
            req.method,
            req.uri,
            req.body.size(),
            escape_json(req.body));

        return AgentResponse::json(200, json);
    }

private:
    // 转义 JSON 字符串中的特殊字符
    static std::string escape_json(const std::string& s) {
        std::string result;
        result.reserve(s.size());
        for (char c : s) {
            switch (c) {
                case '"':  result += "\\\""; break;
                case '\\': result += "\\\\"; break;
                case '\n': result += "\\n";   break;
                case '\r': result += "\\r";   break;
                case '\t': result += "\\t";   break;
                default:   result += c;       break;
            }
        }
        return result;
    }
};

} // namespace agent
} // namespace cyrus
