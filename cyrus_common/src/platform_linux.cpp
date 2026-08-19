// ============================================================================
// platform_linux.cpp - Linux 平台信号处理实现
// ============================================================================
#include "cyrus/platform.hpp"

#include <csignal>

namespace cyrus {

// 全局运行标志 (定义)
std::atomic<bool> g_running{true};

#if CYRUS_PLATFORM_LINUX

static void linux_signal_handler(int /*signum*/) {
    g_running.store(false, std::memory_order_release);
}

void register_signal_handler(SignalCallback /*callback*/) {
    std::signal(SIGINT, linux_signal_handler);
    std::signal(SIGTERM, linux_signal_handler);
}

#endif

} // namespace cyrus
