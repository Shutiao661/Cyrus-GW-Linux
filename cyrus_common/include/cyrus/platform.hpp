// ============================================================================
// platform.hpp - 平台抽象层 (Platform Abstraction Layer)
// ============================================================================
// 这是整个项目最重要的头文件。所有其他文件都通过此文件访问系统 API。
// 它负责:
//   1. OS 检测 (Windows / Linux)
//   2. 网络头文件引入 (Windows: Winsock2 → Linux: POSIX sockets)
//   3. 类型别名 (socket_t, ssize_t 等平台差异类型)
//   4. 函数宏适配 (close/closesocket, sleep/Sleep 等)
//   5. FAKE_URING 存根 (非 Linux 平台提供空实现, 让 io_uring 代码能编译)
//   6. WSA 初始化/清理 (Windows 需要, Linux 不需要)
//   7. 信号处理抽象 (SIGINT/SIGTERM → SetConsoleCtrlHandler)
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
// 第 1 部分: OS 检测宏
// ============================================================================

#ifdef _WIN32
    #define CYRUS_PLATFORM_WINDOWS 1
    #ifndef CYRUS_PLATFORM_LINUX
        #define CYRUS_PLATFORM_LINUX 0
    #endif
#elif defined(__linux__)
    #define CYRUS_PLATFORM_LINUX 1
    #ifndef CYRUS_PLATFORM_WINDOWS
        #define CYRUS_PLATFORM_WINDOWS 0
    #endif
#else
    #error "Unsupported platform: Cyrus-GW currently supports Windows and Linux only."
#endif

// ============================================================================
// 第 2 部分: Windows 平台头文件引入
// ============================================================================
// 注意引入顺序至关重要:
//   1. 先定义 WIN32_LEAN_AND_MEAN + NOMINMAX (防止 windows.h 引入 GDI / 破坏 std::min/max)
//   2. windows.h (提供 HANDLE, DWORD 等 Windows 基础类型)
//   3. winsock2.h (提供 socket, bind, listen, accept, send, recv 等网络函数)
//   4. ws2tcpip.h (提供 getaddrinfo, inet_pton 等现代网络函数)
//   5. mswsock.h (提供 AcceptEx, GetAcceptExSockaddrs 等扩展函数)
// 如果 winsock2.h 在 windows.h 之前引入 → 大量宏重定义编译错误
// ============================================================================

#if CYRUS_PLATFORM_WINDOWS
    // 防止 windows.h 引入太多不必要的内容
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    // 防止 windows.h 的 min/max 宏破坏 std::min/std::max
    #ifndef NOMINMAX
        #define NOMINMAX
    #endif

    #include <windows.h>
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #include <mswsock.h>

    // Windows 上还需要这些
    #include <io.h>       // _read, _write
    #include <process.h>  // _getpid

    // 告诉链接器需要这些 Windows 库 (通过 pragma 确保即使 CMake 遗漏也能链接)
    #pragma comment(lib, "ws2_32.lib")
    #pragma comment(lib, "mswsock.lib")
#endif

// ============================================================================
// 第 3 部分: Linux 平台头文件引入
// ============================================================================

#if CYRUS_PLATFORM_LINUX
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

    // Linux 上的 io_uring (如果可用)
    #if __has_include(<liburing.h>)
        #include <liburing.h>
        #define CYRUS_HAS_LIBURING 1
    #else
        #define CYRUS_HAS_LIBURING 0
    #endif
#endif

// ============================================================================
// 第 4 部分: 跨平台类型别名
// ============================================================================
// socket_t: Linux 上 socket fd 是 int, Windows 上是 SOCKET (即 UINT_PTR)
// 统一的类型别名让路由/连接的代码不需要 #ifdef

#if CYRUS_PLATFORM_WINDOWS
    using socket_t = SOCKET;
    constexpr socket_t INVALID_SOCKET_VAL = INVALID_SOCKET;
    constexpr int SOCKET_ERROR_VAL = SOCKET_ERROR;
#else
    using socket_t = int;
    constexpr socket_t INVALID_SOCKET_VAL = -1;
    constexpr int SOCKET_ERROR_VAL = -1;
#endif

// ============================================================================
// 第 5 部分: 跨平台函数适配宏
// ============================================================================

#if CYRUS_PLATFORM_WINDOWS
    // Windows 用 closesocket 关闭套接字, Linux 用 close
    #define cyrus_close_socket(fd)  ::closesocket(fd)

    // Windows 用 ioctlsocket 设置非阻塞, Linux 用 fcntl
    #define cyrus_set_nonblocking(fd) \
        do { \
            u_long mode = 1; \
            ::ioctlsocket(fd, FIONBIO, &mode); \
        } while(0)

    // 获取最后一次网络错误码
    #define cyrus_socket_error()  ::WSAGetLastError()

    // 错误码常量
    #define CYRUS_EWOULDBLOCK  WSAEWOULDBLOCK
    #define CYRUS_EINPROGRESS  WSAEWOULDBLOCK  // Windows 连接中
    #define CYRUS_ECONNRESET   WSAECONNRESET
    #define CYRUS_EINTR        WSAEINTR

    // 休眠 (毫秒)
    #define cyrus_sleep_ms(ms)  ::Sleep(ms)

    // 字符串大小写比较
    #define cyrus_strcasecmp   _stricmp
    #define cyrus_strncasecmp  _strnicmp

    // 进程 ID
    inline int cyrus_getpid() { return static_cast<int>(::GetCurrentProcessId()); }

    // Windows 没有 ssize_t, 用 SSIZE_T (定义在 Windows.h 中)
    using ssize_t = SSIZE_T;

#else
    // Linux 原生调用
    #define cyrus_close_socket(fd)  ::close(fd)

    #define cyrus_set_nonblocking(fd) \
        do { \
            int flags = ::fcntl(fd, F_GETFL, 0); \
            ::fcntl(fd, F_SETFL, flags | O_NONBLOCK); \
        } while(0)

    #define cyrus_socket_error()  (errno)

    #define CYRUS_EWOULDBLOCK  EWOULDBLOCK
    #define CYRUS_EINPROGRESS  EINPROGRESS
    #define CYRUS_ECONNRESET   ECONNRESET
    #define CYRUS_EINTR        EINTR

    #define cyrus_sleep_ms(ms)  ::usleep((ms) * 1000)

    #define cyrus_strcasecmp   ::strcasecmp
    #define cyrus_strncasecmp  ::strncasecmp

    inline int cyrus_getpid() { return static_cast<int>(::getpid()); }
#endif

// ============================================================================
// 第 6 部分: FAKE_URING 存根 (非 Linux 平台的 io_uring 空实现)
// ============================================================================
// io_uring 是 Linux 5.1+ 独有的高性能异步 I/O 接口
// 在 Windows 上, 我们提供宏存根, 让 io_engine_uring.cpp 能够编译
// 但在运行时会返回错误 —— 实际使用 IOEngineIocp 替代
// 如果强行在 Windows 上运行 io_uring 代码, 会得到清晰的 "not supported" 错误

#if !CYRUS_HAS_LIBURING

// io_uring 核心结构体 (存根, 只用于编译 — liburing 不可用)
struct io_uring {
    int ring_fd = -1;
};
struct io_uring_sqe {
    uint64_t user_data = 0;
};
struct io_uring_cqe {
    uint64_t user_data = 0;
    int32_t  res = 0;
    uint32_t flags = 0;
};
// 注意: 子结构体必须先于引用它们的结构体定义 (MSVC v19.51+ 要求)
struct io_sqring_offsets {
    uint32_t head = 0, tail = 0, ring_mask = 0, ring_entries = 0;
    uint32_t flags = 0, dropped = 0, array = 0;
    uint32_t resv1 = 0, resv2 = 0;
};
struct io_cqring_offsets {
    uint32_t head = 0, tail = 0, ring_mask = 0, ring_entries = 0;
    uint32_t overflow = 0, cqes = 0, flags = 0;
    uint32_t resv1 = 0, resv2 = 0;
};
struct io_uring_params {
    uint32_t flags = 0;
    uint32_t sq_thread_cpu = 0;
    uint32_t sq_thread_idle = 0;
    uint32_t features = 0;
    uint32_t wq_fd = 0;
    uint32_t resv[3] = {0};
    struct io_sqring_offsets sq_off = {};
    struct io_cqring_offsets cq_off = {};
};

// io_uring 操作码常量 (值为 Linux 内核定义)
constexpr uint8_t IORING_OP_ACCEPT  = 13;
constexpr uint8_t IORING_OP_RECV    = 8;
constexpr uint8_t IORING_OP_SEND    = 9;
constexpr uint8_t IORING_OP_SPLICE  = 22;
constexpr uint8_t IORING_OP_SEND_ZC = 23;
constexpr uint8_t IORING_OP_TIMEOUT = 15;
constexpr uint8_t IORING_OP_RECV_MSHOT = 25;
constexpr uint8_t IORING_OP_READ    = 22;
constexpr uint8_t IORING_OP_CANCEL  = 12;
constexpr uint8_t IORING_OP_NOP     = 0;

// 设置 / SQE 标志
constexpr uint32_t IORING_SETUP_SQPOLL   = (1U << 1);
constexpr uint32_t IOSQE_BUFFER_SELECT   = (1U << 7);
constexpr uint32_t IORING_CQE_BUFFER_SHIFT = 16;
constexpr uint32_t IORING_CQE_F_MORE     = (1U << 1);

// --- io_uring 函数存根 (全部返回错误, 让调用者知道不可用) ---

inline int io_uring_queue_init(unsigned, io_uring*, unsigned) {
    return -1;  // 失败: io_uring 在非 Linux 平台上不可用
}

inline io_uring_sqe* io_uring_get_sqe(io_uring*) {
    return nullptr;  // 失败: 无可用 SQE
}

inline void io_uring_sqe_set_data(io_uring_sqe*, void*) {
    // 空操作
}

inline void* io_uring_cqe_get_data(const io_uring_cqe*) {
    return nullptr;
}

inline int io_uring_submit(io_uring*) {
    return -1;
}

inline int io_uring_submit_and_wait(io_uring*, int) {
    return -1;
}

inline int io_uring_wait_cqe(io_uring*, io_uring_cqe**) {
    return -1;
}

inline int io_uring_peek_batch_cqe(io_uring*, io_uring_cqe**, unsigned) {
    return 0;  // 0 个完成事件
}

inline void io_uring_cqe_seen(io_uring*, io_uring_cqe*) {
    // 空操作
}

inline void io_uring_cq_advance(io_uring*, unsigned) {
    // 空操作
}

inline unsigned io_uring_sq_ready(const io_uring*) {
    return 0;
}

inline void io_uring_queue_exit(io_uring*) {
    // 空操作
}

// 用于注册/注销缓冲区的函数
inline int io_uring_register_buffers(io_uring*, const struct iovec*, unsigned) {
    return -1;
}

inline int io_uring_unregister_buffers(io_uring*) {
    return -1;
}

inline int io_uring_register_files(io_uring*, const int*, unsigned) {
    return -1;
}

inline int io_uring_unregister_files(io_uring*) {
    return -1;
}

// SQE 准备函数
inline void io_uring_prep_accept(io_uring_sqe*, int, struct sockaddr*, socklen_t*, int) {}
inline void io_uring_prep_recv(io_uring_sqe*, int, void*, unsigned, int) {}
inline void io_uring_prep_recv_multishot(io_uring_sqe*, int, unsigned, unsigned, int) {}
inline void io_uring_prep_send(io_uring_sqe*, int, const void*, unsigned, int) {}
inline void io_uring_prep_send_zc(io_uring_sqe*, int, const void*, unsigned, int, unsigned) {}
inline void io_uring_prep_splice(io_uring_sqe*, int, int, int, int, unsigned) {}
inline void io_uring_prep_timeout(io_uring_sqe*, struct __kernel_timespec*, unsigned, unsigned) {}
inline void io_uring_prep_cancel(io_uring_sqe*, void*, int) {}
inline void io_uring_prep_nop(io_uring_sqe*) {}
inline void io_uring_prep_close(io_uring_sqe*, int) {}
inline void io_uring_prep_read(io_uring_sqe*, int, void*, unsigned, off_t) {}

#endif // !CYRUS_HAS_LIBURING

// ============================================================================
// 第 7 部分: WSA 初始化/清理 (Windows RAII 封装, Linux 空操作)
// ============================================================================
// Windows 要求任何使用 Winsock2 的程序在调用网络函数之前
// 必须调用 WSAStartup 初始化 Winsock2 DLL
// 使用 RAII (Resource Acquisition Is Initialization) 模式确保:
//   - 程序启动时自动调用 WSAStartup
//   - 程序退出时自动调用 WSACleanup
//   - 异常安全 (即使抛异常也会正确清理)

namespace cyrus {

class WSAContext {
public:
    WSAContext() {
#if CYRUS_PLATFORM_WINDOWS
        WSADATA wsa_data = {};
        // MAKEWORD(2,2) → 请求 Winsock 2.2 版本
        int result = ::WSAStartup(MAKEWORD(2, 2), &wsa_data);
        if (result != 0) {
            throw std::runtime_error("WSAStartup failed with error: " + std::to_string(result));
        }
        initialized_ = true;
#endif
    }

    ~WSAContext() {
#if CYRUS_PLATFORM_WINDOWS
        if (initialized_) {
            ::WSACleanup();
        }
#endif
    }

    // 禁止拷贝 (资源所有权唯一)
    WSAContext(const WSAContext&) = delete;
    WSAContext& operator=(const WSAContext&) = delete;

    // 允许移动
    WSAContext(WSAContext&& other) noexcept {
        initialized_ = other.initialized_;
        other.initialized_ = false;
    }
    WSAContext& operator=(WSAContext&& other) noexcept {
        if (this != &other) {
            initialized_ = other.initialized_;
            other.initialized_ = false;
        }
        return *this;
    }

private:
    bool initialized_ = false;
};

// ============================================================================
// 第 8 部分: 信号处理抽象
// ============================================================================
// Linux 使用 sigaction 注册 SIGINT/SIGTERM 处理器
// Windows 使用 SetConsoleCtrlHandler 注册控制台事件处理器
// 统一为 register_signal_handler(callback) 接口

using SignalCallback = std::function<void()>;

// 注册信号处理器 (Ctrl+C / 关闭控制台)
// callback: 收到信号时调用的回调函数
void register_signal_handler(SignalCallback callback);

// 全局运行标志 (信号处理器设置此值为 false)
extern std::atomic<bool> g_running;

} // namespace cyrus
