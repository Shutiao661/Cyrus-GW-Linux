// ============================================================================
// logger.hpp - 线程安全的日志框架
// ============================================================================
// 功能:
//   - 多级别日志: DEBUG, INFO, WARN, ERROR, FATAL
//   - 线程安全: 使用 mutex 保护输出流
//   - C++20 std::format 风格格式化
//   - 自动添加时间戳、日志级别、源文件位置
//   - 可配置最低日志级别, 输出到 stdout
//
// 使用宏:
//   LOG_DEBUG("Received {} bytes from client {}", bytes, client_id);
//   LOG_INFO("Server started on port {}", port);
//   LOG_ERROR("Failed to connect to {}:{} err={}", host, port, error);
//
// 日志格式:
//   [2024-01-15 14:30:22.123] [INFO ] [server.cpp:42] Server started on port 8080
// ============================================================================

#pragma once

#include "types.hpp"

#include <string>
#include <string_view>
#include <mutex>
#include <cstdio>
#include <format>
#include <source_location>

namespace cyrus {

// ============================================================================
// Logger 类 - 单例模式
// ============================================================================
class Logger {
public:
    // --- 获取单例实例 ---
    static Logger& instance() {
        static Logger logger;
        return logger;
    }

    // --- 配置 ---

    // 设置最低日志级别 (低于此级别的日志将被忽略)
    // 默认: DEBUG (即输出所有日志)
    void set_level(LogLevel level) {
        std::lock_guard<std::mutex> lock(mutex_);
        min_level_ = level;
    }

    // --- 核心日志函数 ---
    // 通常不直接调用, 使用下方的 LOG_* 宏
    // level: 日志级别
    // loc: 源码位置 (由 std::source_location 自动捕获)
    // fmt: 格式字符串 (std::format 风格)
    // args: 格式参数
    template<typename... Args>
    void log(LogLevel level,
             const std::source_location& loc,
             std::string_view fmt,
             Args&&... args)
    {
        // 级别过滤 (快速路径: 如果日志级别不够, 立即返回)
        if (level < min_level_) return;

        // 格式化用户消息
        std::string message;
        try {
            message = std::vformat(fmt, std::make_format_args(args...));
        } catch (const std::format_error&) {
            message = std::string(fmt);  // 格式化失败, 回退到原始字符串
        }

        // 构建完整日志行
        std::string line = format_log_line(level, loc, message);

        // 线程安全输出
        std::lock_guard<std::mutex> lock(mutex_);
        fwrite(line.data(), 1, line.size(), stdout);

        // WARN 及以上立即落盘: stdout 被重定向到文件/管道时 stdio 是全缓冲,
        // 运行中 tail 不到任何内容, 进程被强杀还会整体丢失。
        // 错误日志稀少, 这点开销可忽略; INFO/DEBUG 保持缓冲以维持吞吐。
        if (level >= LogLevel::WARN) {
            fflush(stdout);
        }
    }

private:
    Logger() = default;

    // 禁止拷贝
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    // --- 构建格式化的日志行 ---
    // [2024-01-15 14:30:22.123] [INFO ] [server.cpp:42] message
    std::string format_log_line(LogLevel level,
                                const std::source_location& loc,
                                const std::string& message)
    {
        // 获取当前时间
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      now.time_since_epoch()) % 1000;

        // 分解为本地时间
        struct tm local_time;
        localtime_r(&time_t_now, &local_time);

        // 格式化时间戳: YYYY-MM-DD HH:MM:SS.mmm
        char time_buf[64];
        snprintf(time_buf, sizeof(time_buf),
                 "%04d-%02d-%02d %02d:%02d:%02d.%03lld",
                 local_time.tm_year + 1900,
                 local_time.tm_mon + 1,
                 local_time.tm_mday,
                 local_time.tm_hour,
                 local_time.tm_min,
                 local_time.tm_sec,
                 static_cast<long long>(ms.count()));

        // 从完整路径中提取文件名 (仅文件名, 不含目录)
        const char* filename = loc.file_name();
        const char* last_sep = filename;
        for (const char* p = filename; *p; ++p) {
            if (*p == '/' || *p == '\\') last_sep = p + 1;
        }

        // 构建: [时间] [级别] [文件:行号] 消息
        // 使用 std::format 进行高效的字符串拼接
        return std::format("[{}] [{:<5}] [{}:{}] {}\n",
                          time_buf,
                          log_level_to_string(level),
                          last_sep,
                          loc.line(),
                          message);
    }

    LogLevel min_level_{LogLevel::DEBUG};          // 最低输出级别
    std::mutex mutex_;                              // 线程安全锁
};

} // namespace cyrus

// ============================================================================
// 便捷日志宏
// ============================================================================
// 使用 std::source_location::current() 自动捕获调用位置
// 不需要手动传 __FILE__ / __LINE__

#define LOG_DEBUG(fmt, ...) \
    cyrus::Logger::instance().log(cyrus::LogLevel::DEBUG, std::source_location::current(), fmt, ##__VA_ARGS__)

#define LOG_INFO(fmt, ...) \
    cyrus::Logger::instance().log(cyrus::LogLevel::INFO, std::source_location::current(), fmt, ##__VA_ARGS__)

#define LOG_WARN(fmt, ...) \
    cyrus::Logger::instance().log(cyrus::LogLevel::WARN, std::source_location::current(), fmt, ##__VA_ARGS__)

#define LOG_ERROR(fmt, ...) \
    cyrus::Logger::instance().log(cyrus::LogLevel::ERROR, std::source_location::current(), fmt, ##__VA_ARGS__)

#define LOG_FATAL(fmt, ...) \
    cyrus::Logger::instance().log(cyrus::LogLevel::FATAL, std::source_location::current(), fmt, ##__VA_ARGS__)
