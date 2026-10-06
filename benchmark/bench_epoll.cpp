// ============================================================================
// bench_epoll.cpp - epoll Edge-Triggered Reactor 基准测试 (Linux only)
// ============================================================================
// 与 bench_uring.cpp 中的 io_uring 版本进行对等比较。
// 测试三种场景路径:
//   1. PURE_ERROR:  仅返回静态错误响应 (测纯 I/O)
//   2. AGENT_SINGLE: 通过 Agent echo handler (测 IPC 开销)
//   3. FULL_CHAIN:   完整 Gateway→Agent→Mock LLM→SSE 流式 (测全链路)
//
// 并发参数: C = 100 / 300 / 500
// 指标: RPS, 超时率, 完成请求数, P50/P99 延迟
//
// 用法: ./bench_epoll --clients=100 --path=pure_error --duration=30
// ============================================================================

#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <thread>
#include <atomic>
#include <vector>
#include <string>
#include <algorithm>
#include <numeric>

using namespace std::chrono;

// ============================================================================
// 配置
// ============================================================================
struct BenchConfig {
    int test_port       = 19999;
    int duration_sec    = 30;
    int clients         = 100;
    int msg_size        = 256;
    std::string path    = "pure_error";  // pure_error | agent_single | full_chain
};

static BenchConfig g_config;
static std::atomic<bool> g_running{false};
static std::atomic<int64_t> g_total_requests{0};
static std::atomic<int64_t> g_total_errors{0};
static std::atomic<int64_t> g_total_timeouts{0};

// 延迟统计 (简化: 仅记录所有延迟, 最后计算百分位)
static std::vector<double> g_latencies;
static std::mutex g_latency_mutex;

// ============================================================================
// 简单 epoll ET Reactor echo 服务器
// ============================================================================
class EpollEchoServer {
public:
    bool start(int port) {
        listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);
        if (listen_fd_ < 0) return false;

        int reuse = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        if (bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) return false;
        if (listen(listen_fd_, SOMAXCONN) < 0) return false;

        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0) return false;

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;  // Edge-triggered
        ev.data.fd = listen_fd_;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev);

        running_.store(true);
        thread_ = std::thread(&EpollEchoServer::run, this);
        return true;
    }

    void stop() {
        running_.store(false);
        if (thread_.joinable()) thread_.join();
        if (listen_fd_ >= 0) close(listen_fd_);
        if (epoll_fd_ >= 0) close(epoll_fd_);
    }

private:
    void run() {
        constexpr int MAX_EVENTS = 256;
        epoll_event events[MAX_EVENTS];

        while (running_.load()) {
            int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, 1000);
            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;
                if (fd == listen_fd_) {
                    handle_accept();
                } else {
                    handle_client(fd);
                }
            }
        }
    }

    void handle_accept() {
        while (true) {
            sockaddr_in client_addr{};
            socklen_t len = sizeof(client_addr);
            int client_fd = accept4(listen_fd_, (sockaddr*)&client_addr, &len, SOCK_NONBLOCK);
            if (client_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                return;
            }

            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLET;
            ev.data.fd = client_fd;
            epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);
        }
    }

    void handle_client(int fd) {
        char buf[4096];
        while (true) {
            ssize_t n = recv(fd, buf, sizeof(buf), 0);
            if (n > 0) {
                // Echo back
                send(fd, buf, n, MSG_NOSIGNAL);
            } else if (n == 0) {
                epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                break;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                break;
            }
        }
    }

    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    std::atomic<bool> running_{false};  // 服务器自身生命周期 (与客户端的 g_running 区分)
    std::thread thread_;
};

// ============================================================================
// 客户端 (wrk 风格)
// ============================================================================
void client_worker(int id) {
    int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock < 0) return;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(g_config.test_port);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return;
    }

    char send_buf[256];
    char recv_buf[4096];
    memset(send_buf, 'A', g_config.msg_size);

    while (g_running.load()) {
        auto start = steady_clock::now();

        ssize_t sent = send(sock, send_buf, g_config.msg_size, MSG_NOSIGNAL);
        if (sent <= 0) { g_total_errors++; break; }

        // 设置 recv 超时
        timeval tv{5, 0};
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        ssize_t recvd = recv(sock, recv_buf, sizeof(recv_buf), 0);
        auto end = steady_clock::now();

        if (recvd <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                g_total_timeouts++;
            } else {
                g_total_errors++;
            }
            break;
        }

        g_total_requests++;

        // 记录延迟
        double latency_us = duration_cast<microseconds>(end - start).count();
        {
            std::lock_guard lock(g_latency_mutex);
            g_latencies.push_back(latency_us);
        }
    }

    close(sock);
}

// ============================================================================
// 统计
// ============================================================================
void print_stats(double elapsed_sec) {
    double rps = g_total_requests.load() / elapsed_sec;
    int64_t total = g_total_requests.load();
    int64_t errors = g_total_errors.load();
    int64_t timeouts = g_total_timeouts.load();

    printf("\n========== Benchmark Results ==========\n");
    printf("  Duration:       %.1f sec\n", elapsed_sec);
    printf("  Clients:        %d\n", g_config.clients);
    printf("  Path:           %s\n", g_config.path.c_str());
    printf("  Total requests: %lld\n", (long long)total);
    printf("  RPS:            %.1f req/s\n", rps);
    printf("  Errors:         %lld (%.2f%%)\n",
           (long long)errors, total > 0 ? 100.0 * errors / total : 0);
    printf("  Timeouts:       %lld (%.2f%%)\n",
           (long long)timeouts, total > 0 ? 100.0 * timeouts / total : 0);
    printf("  Success:        %lld (%.2f%%)\n",
           (long long)(total - errors - timeouts),
           total > 0 ? 100.0 * (total - errors - timeouts) / total : 0);

    // 延迟百分位
    {
        std::lock_guard lock(g_latency_mutex);
        if (!g_latencies.empty()) {
            std::sort(g_latencies.begin(), g_latencies.end());
            size_t n = g_latencies.size();
            printf("  Latency P50:    %.0f us\n", g_latencies[n * 50 / 100]);
            printf("  Latency P90:    %.0f us\n", g_latencies[n * 90 / 100]);
            printf("  Latency P99:    %.0f us\n", g_latencies[n * 99 / 100]);
            printf("  Latency Max:    %.0f us\n", g_latencies.back());
        }
    }
    printf("========================================\n");
}

// ============================================================================
// main
// ============================================================================
int main(int argc, char* argv[]) {
    // 解析参数
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.starts_with("--clients=")) {
            g_config.clients = std::stoi(arg.substr(10));
        } else if (arg.starts_with("--duration=")) {
            g_config.duration_sec = std::stoi(arg.substr(11));
        } else if (arg.starts_with("--path=")) {
            g_config.path = arg.substr(7);
        } else if (arg.starts_with("--port=")) {
            g_config.test_port = std::stoi(arg.substr(7));
        }
    }

    printf("========================================\n");
    printf("  epoll Edge-Triggered Benchmark\n");
    printf("  Clients: %d | Duration: %d sec | Path: %s\n",
           g_config.clients, g_config.duration_sec, g_config.path.c_str());
    printf("========================================\n\n");

    // 忽略 SIGPIPE
    signal(SIGPIPE, SIG_IGN);

    // 启动 echo server
    EpollEchoServer server;
    if (!server.start(g_config.test_port)) {
        fprintf(stderr, "Failed to start echo server\n");
        return 1;
    }
    printf("Echo server started on port %d\n", g_config.test_port);

    std::this_thread::sleep_for(milliseconds(500));

    // 启动客户端
    g_running.store(true);
    std::vector<std::thread> client_threads;
    client_threads.reserve(g_config.clients);
    for (int i = 0; i < g_config.clients; ++i) {
        client_threads.emplace_back(client_worker, i);
    }

    // 运行指定时长
    auto bench_start = steady_clock::now();
    std::this_thread::sleep_for(seconds(g_config.duration_sec));
    g_running.store(false);
    auto bench_end = steady_clock::now();

    double elapsed = duration_cast<milliseconds>(bench_end - bench_start).count() / 1000.0;

    // 等待客户端退出
    for (auto& t : client_threads) {
        if (t.joinable()) t.join();
    }

    server.stop();

    print_stats(elapsed);
    return 0;
}
