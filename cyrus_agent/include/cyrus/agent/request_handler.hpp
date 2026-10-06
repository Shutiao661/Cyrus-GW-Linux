// ============================================================================
// request_handler.hpp - Agent 请求处理器接口
// ============================================================================
// 策略模式 (Strategy Pattern): 不同的 URI 使用不同的处理器
// 当前实现:
//   - EchoHandler:  回显客户端请求 (用于调试)
//   - ChatHandler:  模拟 LLM 聊天补全 (SSE 流式响应)
// ============================================================================

#pragma once

#include "cyrus/types.hpp"

#include <string>
#include <vector>
#include <memory>

namespace cyrus {
namespace agent {

// ============================================================================
// AgentRequest - Agent 请求结构
// ============================================================================
struct AgentRequest {
    std::string method;   // HTTP 方法 (如 "POST")
    std::string uri;      // 请求 URI (如 "/v1/chat/completions")
    std::string body;     // 请求体 (JSON 字符串)

    // 从请求体解析 JSON (简化: 查找 key 的值)
    std::string json_value(const std::string& key) const;
};

// ============================================================================
// AgentResponse - Agent 响应结构
// ============================================================================
struct AgentResponse {
    int status = 200;                              // HTTP 状态码
    std::string body;                              // 响应体 (单次响应)
    bool stream = false;                           // 是否为流式响应?
    std::vector<std::string> stream_chunks;        // 流式数据块

    // 工厂方法: 创建 JSON 响应
    static AgentResponse json(int status, std::string json_body) {
        AgentResponse resp;
        resp.status = status;
        resp.body = std::move(json_body);
        return resp;
    }

    // 工厂方法: 创建流式响应
    static AgentResponse streaming(int status, std::vector<std::string> chunks) {
        AgentResponse resp;
        resp.status = status;
        resp.stream = true;
        resp.stream_chunks = std::move(chunks);
        return resp;
    }
};

// ============================================================================
// RequestHandler - 请求处理器抽象接口
// ============================================================================
class RequestHandler {
public:
    virtual ~RequestHandler() = default;

    // 处理请求并返回响应
    // req: 解析后的请求
    // 返回: AgentResponse
    virtual AgentResponse handle(const AgentRequest& req) = 0;
};

using RequestHandlerPtr = std::unique_ptr<RequestHandler>;

// ============================================================================
// AgentRequest::json_value() 实现
// ============================================================================
// 极简 JSON 解析: 查找 "key" 并在其后提取字符串值
// 仅用于演示, 生产环境应使用 nlohmann/json 或 simdjson
inline std::string AgentRequest::json_value(const std::string& key) const {
    // 查找 "\"key\""
    std::string search = "\"" + key + "\"";
    auto pos = body.find(search);
    if (pos == std::string::npos) return "";

    // 跳到冒号后面
    pos = body.find(':', pos + search.size());
    if (pos == std::string::npos) return "";

    // 跳过空白和引号
    pos++;
    while (pos < body.size() && (body[pos] == ' ' || body[pos] == '\t' || body[pos] == '\n')) {
        pos++;
    }

    if (pos >= body.size()) return "";

    if (body[pos] == '"') {
        // 字符串值
        pos++;  // 跳过开始引号
        auto end_pos = body.find('"', pos);
        if (end_pos == std::string::npos) return "";
        return body.substr(pos, end_pos - pos);
    } else {
        // 数字或布尔值
        auto end_pos = body.find_first_of(",}\n\r\t ", pos);
        if (end_pos == std::string::npos) end_pos = body.size();
        return body.substr(pos, end_pos - pos);
    }
}

} // namespace agent
} // namespace cyrus
