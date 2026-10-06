// ============================================================================
// chat_handler.hpp - Chat Completion 请求处理器
// ============================================================================
// 支持可插拔的 LLM Provider 架构:
//   - LLMProvider 抽象接口 (OpenAI / 本地模型 / Mock)
//   - MockLLMProvider: 测试用模拟实现
//   - ChatHandler: 接收 Gateway 请求 → 调用 LLM Provider → 返回 SSE 流
//
// 请求: POST /v1/chat/completions
//   Body: {"model":"...","messages":[{"role":"user","content":"Hello"}]}
//
// 响应: 流式 SSE 事件
//   data: {"choices":[{"delta":{"content":"Hello"}}]}
//   data: [DONE]
// ============================================================================

#pragma once

#include "request_handler.hpp"
#include "cyrus/sse_codec.hpp"

#include <vector>
#include <string>
#include <chrono>
#include <random>
#include <sstream>
#include <memory>

namespace cyrus {
namespace agent {

// ============================================================================
// LLMProvider - LLM 调用抽象接口 (策略模式)
// ============================================================================
class LLMProvider {
public:
    virtual ~LLMProvider() = default;

    // 生成 token 序列
    // prompt: 用户输入的文本
    // 返回: SSE 格式的 chunk 列表 (每个 chunk 是一个完整的 SSE data: 行)
    virtual std::vector<std::string> generate(const std::string& prompt) = 0;
};

// ============================================================================
// MockLLMProvider - 模拟 LLM (演示用)
// ============================================================================
class MockLLMProvider : public LLMProvider {
public:
    std::vector<std::string> generate(const std::string& prompt) override {
        std::vector<std::string> tokens = generate_mock_tokens(prompt);

        std::vector<std::string> chunks;
        chunks.reserve(tokens.size());
        for (const auto& token : tokens) {
            chunks.push_back(SSEFormatter::completion_chunk(build_chunk_json(token)));
        }
        // 不在此处发 [DONE]: 流的结束由协议层 MSG_RESPONSE_END 表达,
        // Gateway 的中继器会在流结束时统一补一个 [DONE]
        // (两边都发会让客户端收到两次 [DONE])。
        return chunks;
    }

private:
    std::vector<std::string> generate_mock_tokens(const std::string& user_input) {
        const std::vector<std::string> mock_responses[] = {
            {"Hello", " there", "!", " I", "'m", " Cyrus", "-GW", "'s", " AI", " assistant",
             ".", " How", " can", " I", " help", " you", " today", "?"},
            {"你好", "！", "我", "是", "Cyrus", "-GW", "内置", "的", "AI", "助手",
             "。", "有", "什么", "可以", "帮助", "你", "的", "吗", "？"},
            {"Cyrus", "-GW", " is", " a", " high", "-performance", " async", " HTTP",
             " gateway", " built", " with", " C++", "20", " and", " io_uring",
             ".", " It", " supports", " SSE", " streaming", " and", " binary", " protocol",
             " communication", " with", " backend", " agents", "."}
        };

        size_t index = 0;
        for (char c : user_input) {
            if (static_cast<unsigned char>(c) > 127) { index = 1; break; }
        }
        return mock_responses[index];
    }

    static std::string build_chunk_json(const std::string& content) {
        std::ostringstream oss;
        oss << R"({"choices":[{"delta":{"content":")"
            << escape_json(content)
            << R"("},"index":0}]})";
        return oss.str();
    }

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

// ============================================================================
// ChatHandler - 聊天补全处理器 (支持可插拔 LLM Provider)
// ============================================================================
class ChatHandler : public RequestHandler {
public:
    // 默认使用 Mock provider
    ChatHandler() : provider_(std::make_unique<MockLLMProvider>()) {}

    // 注入自定义 provider
    explicit ChatHandler(std::unique_ptr<LLMProvider> provider)
        : provider_(std::move(provider)) {}

    AgentResponse handle(const AgentRequest& req) override {
        std::string user_message = req.json_value("content");
        if (user_message.empty()) {
            user_message = "Hello";
        }

        std::vector<std::string> chunks = provider_->generate(user_message);
        return AgentResponse::streaming(200, chunks);
    }

private:
    std::unique_ptr<LLMProvider> provider_;
};

} // namespace agent
} // namespace cyrus
