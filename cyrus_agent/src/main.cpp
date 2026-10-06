// ============================================================================
// main.cpp - Cyrus Agent 入口点
// ============================================================================
// 命令行: cyrus_agent [--port N] [--config file]
//   默认端口: 9999
//
// Agent 生命周期:
//   1. 解析命令行参数
//   2. 创建 AgentServer
//   3. 注册请求处理器 (Echo + Chat)
//   4. 启动服务器
//   5. 等待 Ctrl+C 关闭信号
//   6. 优雅退出
// ============================================================================

#include "cyrus/agent/agent_server.hpp"
#include "cyrus/agent/echo_handler.hpp"
#include "cyrus/agent/chat_handler.hpp"
#include "cyrus/agent/deepseek_provider.hpp"
#include "cyrus/logger.hpp"

#include <string>
#include <cstring>
#include <cstdlib>

using namespace cyrus;
using namespace cyrus::agent;

int main(int argc, char* argv[]) {
    // ========================================================================
    // 第 1 步: 解析命令行参数
    // ========================================================================
    uint16_t port = 9999;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        }
    }

    // ========================================================================
    // 第 2 步: 配置日志
    // ========================================================================
    Logger::instance().set_level(LogLevel::INFO);

    LOG_INFO("============================================");
    LOG_INFO("  Cyrus Agent v2.0.0");
    LOG_INFO("  Platform: Linux");
    LOG_INFO("============================================");

    // ========================================================================
    // 第 3 步: 注册信号处理器
    // ========================================================================
    register_signal_handler([]() {
        LOG_INFO("Agent received shutdown signal");
    });

    // ========================================================================
    // 第 4 步: 创建 Agent 服务器
    // ========================================================================
    AgentServer server(port);

    // 注册请求处理器
    server.register_handler("/echo", std::make_unique<EchoHandler>());

    // DeepSeek API key 从环境变量读取 (绝不硬编码进源码, 避免泄露)。
    // 未设置 DEEPSEEK_API_KEY 时回退到 Mock provider (本地演示, 无需网络)。
    const char* api_key_env = std::getenv("DEEPSEEK_API_KEY");
    std::string api_key = (api_key_env != nullptr) ? api_key_env : "";

    if (!api_key.empty()) {
        auto deepseek = std::make_unique<DeepSeekProvider>(api_key, "deepseek-chat");
        server.register_handler("/v1/chat/completions",
            std::make_unique<ChatHandler>(std::move(deepseek)));
        server.register_handler("/v1/chat",
            std::make_unique<ChatHandler>(
                std::make_unique<DeepSeekProvider>(api_key, "deepseek-chat")));
        LOG_INFO("Registered DeepSeek chat handler (real LLM)");
    } else {
        server.register_handler("/v1/chat/completions",
            std::make_unique<ChatHandler>());  // 默认 Mock provider
        server.register_handler("/v1/chat",
            std::make_unique<ChatHandler>());
        LOG_WARN("DEEPSEEK_API_KEY not set — falling back to Mock LLM provider");
    }

    // ========================================================================
    // 第 5 步: 启动服务器
    // ========================================================================
    if (!server.start()) {
        LOG_FATAL("Failed to start agent server. Exiting.");
        return 1;
    }

    LOG_INFO("Agent ready. Port: {}, Handlers: echo, chat", port);

    // ========================================================================
    // 第 6 步: 等待关闭信号
    // ========================================================================
    server.wait_for_shutdown();

    LOG_INFO("Cyrus Agent stopped. Goodbye!");
    return 0;
}
