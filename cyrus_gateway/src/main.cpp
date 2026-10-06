// ============================================================================
// main.cpp - Cyrus-GW 网关入口点
// ============================================================================
// 命令行: cyrus_gateway [config_file]
//   默认配置文件: config/gateway.conf
//
// 生命周期:
//   1. 解析命令行参数
//   2. 加载配置文件
//   3. 注册 Ctrl+C 信号处理器
//   4. 创建并启动 Server
//   5. 等待关闭信号
//   6. 优雅退出
// ============================================================================

#include "cyrus/gateway/server.hpp"
#include "cyrus/logger.hpp"

using namespace cyrus;
using namespace cyrus::gateway;

int main(int argc, char* argv[]) {
    // ========================================================================
    // 第 1 步: 解析命令行参数
    // ========================================================================
    std::string config_path = "config/gateway.conf";
    if (argc > 1) {
        config_path = argv[1];
    }

    // ========================================================================
    // 第 2 步: 配置日志
    // ========================================================================
    Logger::instance().set_level(LogLevel::DEBUG);
    LOG_INFO("============================================");
    LOG_INFO("  Cyrus-GW Gateway v2.0.0");
    LOG_INFO("  Platform: Linux (io_uring)");
    LOG_INFO("============================================");

    // ========================================================================
    // 第 3 步: 加载配置文件
    // ========================================================================
    Config config;
    if (!config.load(config_path)) {
        LOG_WARN("Failed to load config '{}', using defaults", config_path);
    }

    // 从配置文件读取日志级别 (如果存在)
    auto log_level_str = config.get("logging", "level", "DEBUG");
    if (log_level_str == "DEBUG") Logger::instance().set_level(LogLevel::DEBUG);
    else if (log_level_str == "INFO")  Logger::instance().set_level(LogLevel::INFO);
    else if (log_level_str == "WARN")  Logger::instance().set_level(LogLevel::WARN);
    else if (log_level_str == "ERROR") Logger::instance().set_level(LogLevel::ERROR);

    // ========================================================================
    // 第 4 步: 注册信号处理器 (Ctrl+C)
    // ========================================================================
    register_signal_handler([]() {
        LOG_INFO("Received shutdown signal (Ctrl+C)");
    });

    // ========================================================================
    // 第 5 步: 创建并启动服务器
    // ========================================================================
    Server server(config);

    if (!server.start()) {
        LOG_FATAL("Failed to start server. Exiting.");
        return 1;
    }

    // ========================================================================
    // 第 6 步: 等待关闭信号
    // ========================================================================
    server.wait_for_shutdown();

    // ========================================================================
    // 第 7 步: 退出
    // ========================================================================
    LOG_INFO("Cyrus-GW Gateway stopped. Goodbye!");
    return 0;
}
