// ============================================================================
// deepseek_provider.hpp - DeepSeek API LLM Provider
// ============================================================================
// 通过 HTTP SSE 调用 DeepSeek API (api.deepseek.com), 解析流式响应。
// 使用 popen("curl ...") 子进程方式, 无需额外 C++ HTTP 库依赖。
// ============================================================================

#pragma once

#include "request_handler.hpp"
#include "cyrus/sse_codec.hpp"
#include "cyrus/logger.hpp"

#include <vector>
#include <string>
#include <sstream>
#include <cstdio>
#include <memory>
#include <regex>

namespace cyrus {
namespace agent {

class DeepSeekProvider : public LLMProvider {
public:
    explicit DeepSeekProvider(std::string api_key, std::string model = "deepseek-chat")
        : api_key_(std::move(api_key))
        , model_(std::move(model))
    {}

    std::vector<std::string> generate(const std::string& prompt) override {
        return call_deepseek_api(prompt);
    }

private:
    std::string api_key_;
    std::string model_;

    // 构建 JSON 请求体
    static std::string build_request_body(const std::string& prompt, const std::string& model) {
        // 转义 prompt 中的特殊 JSON 字符
        std::string escaped;
        escaped.reserve(prompt.size());
        for (char c : prompt) {
            switch (c) {
                case '"':  escaped += "\\\""; break;
                case '\\': escaped += "\\\\"; break;
                case '\n': escaped += "\\n";  break;
                case '\r': escaped += "\\r";  break;
                case '\t': escaped += "\\t";  break;
                default:   escaped += c;      break;
            }
        }

        std::ostringstream oss;
        oss << "{"
            << "\"model\":\"" << model << "\","
            << "\"messages\":[{\"role\":\"user\",\"content\":\"" << escaped << "\"}],"
            << "\"stream\":true"
            << "}";
        return oss.str();
    }

    // 调用 DeepSeek API 并解析 SSE 响应
    std::vector<std::string> call_deepseek_api(const std::string& prompt) {
        std::vector<std::string> chunks;
        std::string body = build_request_body(prompt, model_);

        // 构建 curl 命令行
        // -s: 静默  -N: 禁用缓冲(SSE)  -X POST: POST 方法
        std::ostringstream cmd;
        cmd << "curl -s -N -X POST https://api.deepseek.com/v1/chat/completions"
            << " -H \"Content-Type: application/json\""
            << " -H \"Authorization: Bearer " << api_key_ << "\""
            << " -d '" << body << "'"
            << " --max-time 60"           // 总超时 60s
            << " --connect-timeout 10"     // 连接超时 10s
            << " 2>/dev/null";

        LOG_INFO("DeepSeek: calling API (model={}, prompt_len={})", model_, prompt.size());

        FILE* pipe = popen(cmd.str().c_str(), "r");
        if (!pipe) {
            LOG_ERROR("DeepSeek: popen failed: {}", strerror(errno));
            chunks.push_back(SSEFormatter::completion_chunk(
                R"({"choices":[{"delta":{"content":"[Error: API unreachable]"},"index":0}],"error":"popen failed"})"));
            return chunks;
        }

        // 逐行读取 SSE 响应
        char line_buf[16384];
        std::string data_content;
        int total_tokens = 0;

        while (fgets(line_buf, sizeof(line_buf), pipe) != nullptr) {
            std::string line(line_buf);

            // 去除尾部 \r\n
            while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
                line.pop_back();
            }

            // SSE "data: " 行
            if (line.starts_with("data: ")) {
                std::string data = line.substr(6);  // 跳过 "data: "

                if (data == "[DONE]") {
                    break;  // 流结束
                }

                try {
                    // 提取 delta.content
                    std::string content = extract_delta_content(data);
                    if (!content.empty()) {
                        data_content += content;
                        total_tokens++;
                        std::string chunk_json = build_chunk_json(content);
                        chunks.push_back(SSEFormatter::completion_chunk(chunk_json));
                    }
                } catch (...) {
                    // 解析失败, 跳过此行
                    LOG_DEBUG("DeepSeek: parse error on line: {}", data.substr(0, 80));
                }
            }
            // "event: " 或 "id: " 行忽略
        }

        int exit_code = pclose(pipe);
        LOG_INFO("DeepSeek: API call complete (tokens={}, content_len={}, exit={})",
                 total_tokens, data_content.size(), exit_code);

        if (chunks.empty() || exit_code != 0) {
            // API 调用失败: 生成错误 chunk
            if (chunks.empty()) {
                chunks.push_back(SSEFormatter::completion_chunk(
                    R"({"choices":[{"delta":{"content":"[Error: API call failed]"},"index":0}],"error":"curl exit )"
                    + std::to_string(exit_code) + "\"}"));
            }
        }

        // 结束标记由 Gateway 的中继器在 MSG_RESPONSE_END 时统一补发 (见 chat_handler)
        return chunks;
    }

    // 从 SSE data JSON 中提取 delta.content
    static std::string extract_delta_content(const std::string& json) {
        // 简单字符串搜索 (比引入 json 库更轻量)
        const char* key = "\"content\":\"";
        auto pos = json.find(key);
        if (pos == std::string::npos) return "";

        pos += strlen(key);
        std::string result;
        result.reserve(256);

        while (pos < json.size()) {
            char c = json[pos];
            if (c == '\\' && pos + 1 < json.size()) {
                // 转义字符
                char next = json[pos + 1];
                switch (next) {
                    case '"':  result += '"';  break;
                    case '\\': result += '\\'; break;
                    case 'n':  result += '\n'; break;
                    case 'r':  result += '\r'; break;
                    case 't':  result += '\t'; break;
                    case 'u':  // Unicode: 跳过 \uXXXX
                        pos += 5;  // skip \ and uXXXX
                        continue;  // don't pos++ at end
                    default:   result += next; break;
                }
                pos += 2;
                continue;
            }
            if (c == '"') break;  // 字符串结束
            result += c;
            pos++;
        }
        return result;
    }

    // 构建 OpenAI 兼容的 chunk JSON
    static std::string build_chunk_json(const std::string& content) {
        std::ostringstream oss;
        oss << R"({"choices":[{"delta":{"content":")"
            << escape_json(content)
            << R"("},"index":0}]})";
        return oss.str();
    }

    static std::string escape_json(const std::string& s) {
        std::string result;
        result.reserve(s.size() + 16);
        for (char c : s) {
            switch (c) {
                case '"':  result += "\\\""; break;
                case '\\': result += "\\\\"; break;
                case '\n': result += "\\n";  break;
                case '\r': result += "\\r";  break;
                case '\t': result += "\\t";  break;
                default:   result += c;      break;
            }
        }
        return result;
    }
};

} // namespace agent
} // namespace cyrus
