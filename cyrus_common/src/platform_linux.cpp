// ============================================================================
// platform_linux.cpp - Linux 平台信号处理实现
// ============================================================================
#include "cyrus/platform.hpp"

#include <csignal>

namespace cyrus {

// 全局运行标志 (定义)
std::atomic<bool> g_running{true};

// 用户注册的回调。信号处理器**不**直接调用它 (std::function 在信号上下文中
// 非 async-signal-safe, 可能加锁/分配内存)。由 drain_signal_callback() 在主线程执行。
static SignalCallback g_signal_callback;
static std::atomic<bool> g_signal_pending{false};

static void linux_signal_handler(int /*signum*/) {
    // 只做 async-signal-safe 的原子操作
    g_running.store(false, std::memory_order_release);
    g_signal_pending.store(true, std::memory_order_release);
}

void register_signal_handler(SignalCallback callback) {
    g_signal_callback = std::move(callback);
    std::signal(SIGINT, linux_signal_handler);
    std::signal(SIGTERM, linux_signal_handler);
    // 关键: 忽略 SIGPIPE。向已关闭的 socket 发送数据会触发 SIGPIPE,
    // 默认动作是终止进程。网络守护进程必须忽略它, 改由 ::send 返回 EPIPE 处理。
    std::signal(SIGPIPE, SIG_IGN);
}

void drain_signal_callback() {
    if (g_signal_pending.exchange(false, std::memory_order_acq_rel)) {
        if (g_signal_callback) {
            g_signal_callback();
        }
    }
}

} // namespace cyrus
