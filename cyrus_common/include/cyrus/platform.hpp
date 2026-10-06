// ============================================================================
// platform.hpp - Linux 平台抽象层 (Platform Abstraction Layer)
// ============================================================================
// 本项目仅支持 Linux。所有其他文件都通过此文件访问系统 API。
// 它负责:
//   1. 网络头文件引入 (POSIX sockets)
//   2. 类型别名 (socket_t 等)
//   3. 函数宏适配 (close / fcntl / usleep 等)
//   4. io_uring 检测 (liburing 可用时启用完整实现, 否则回退 POSIX)
//   5. 信号处理抽象 (SIGINT/SIGTERM)
//
// 使用方式: 每个 .cpp 文件第一行 #include "cyrus/platform.hpp"
// ============================================================================

#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>
#include <functional>
#include <memory>
#include <optional>
#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <stdexcept>
#include <system_error>

// ============================================================================
// Linux 平台头文件引入
// ============================================================================

#include <unistd.h>      // close, read, write, getpid, sleep
#include <sys/socket.h>  // socket, bind, listen, accept, send, recv
#include <sys/un.h>      // sockaddr_un (Unix domain sockets)
#include <netinet/in.h>  // sockaddr_in, htonl, ntohl
#include <netinet/tcp.h> // TCP_NODELAY
#include <arpa/inet.h>   // inet_pton, inet_ntop
#include <fcntl.h>       // fcntl (设置非阻塞)
#include <csignal>       // signal, sigaction
#include <sys/epoll.h>   // epoll (仅 Agent 端使用)
#include <errno.h>       // errno
#include <cstring>       // strerror
#include <strings.h>     // strcasecmp / strncasecmp

// io_uring 检测 (liburing 可用时启用完整实现)
#if __has_include(<liburing.h>)
    #include <liburing.h>
    #define CYRUS_HAS_LIBURING 1
#else
    #define CYRUS_HAS_LIBURING 0
#endif

// ============================================================================
// 类型别名
// ============================================================================
// Linux 上 socket fd 是 int。统一的别名让路由/连接的代码保持简洁。

using socket_t = int;
constexpr socket_t INVALID_SOCKET_VAL = -1;
constexpr int SOCKET_ERROR_VAL = -1;

// ============================================================================
// 函数适配宏 (Linux 原生调用)
// ============================================================================

#define cyrus_close_socket(fd)  ::close(fd)

#define cyrus_set_nonblocking(fd) \
    do { \
        int flags = ::fcntl(fd, F_GETFL, 0); \
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK); \
    } while(0)

#define cyrus_socket_error()  (errno)

#define CYRUS_EWOULDBLOCK  EWOULDBLOCK
#define CYRUS_EINTR        EINTR

#define cyrus_sleep_ms(ms)  ::usleep((ms) * 1000)

// ============================================================================
// 信号处理抽象
// ============================================================================
// 使用 sigaction/signal 注册 SIGINT/SIGTERM 处理器, 统一为
// register_signal_handler(callback) 接口。

namespace cyrus {

using SignalCallback = std::function<void()>;

// 注册信号处理器 (Ctrl+C / SIGTERM)
// callback: 收到信号时调用的回调函数
void register_signal_handler(SignalCallback callback);

// 在主线程安全地调用一次已注册的回调 (信号处理器只做原子操作,
// 真正的回调逻辑延后到此处执行, 避免在信号上下文中做非 async-signal-safe 的事)
void drain_signal_callback();

// 全局运行标志 (信号处理器设置此值为 false)
extern std::atomic<bool> g_running;

} // namespace cyrus
