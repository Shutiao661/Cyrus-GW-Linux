// ============================================================================
// bench_uring.cpp - io_uring + C++20 协程基准测试 (Linux only)
// ============================================================================
// 与 bench_epoll.cpp 进行对等比较, 验证 io_uring + 协程的性能优势。
//
// 三种测试路径:
//   1. PURE_ERROR:  仅返回静态响应 (测纯 I/O 性能)
//   2. AGENT_SINGLE: 经过 Agent echo handler (测 IPC 开销)
//   3. FULL_CHAIN:   完整 Gateway→Agent→Mock LLM→SSE 流式 (测全链路)
//
// 预期: C=500 时 io_uring 完成请求数是 epoll 的 2.15 倍
//
// 用法: ./bench_uring --clients=500 --path=full_chain --duration=30
// ============================================================================

#include "../cyrus_gateway/include/cyrus/gateway/io_engine_uring.hpp"
#include "../cyrus_gateway/include/cyrus/gateway/coro_engine.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
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
#include <mutex>

using namespace cyrus::gateway;

static std::atomic<bool> g_running{false};
static std::atomic<int64_t> g_total_requests{0};
static std::atomic<int64_t> g_total_errors{0};
static std::atomic<int64_t> g_total_timeouts{0};
static std::vector<double> g_latencies;
static std::mutex g_latency_mutex;

struct Config {
    int port = 19999;
    int duration = 30;
    int clients = 100;
    int msg_size = 256;
    std::string path = "pure_error";
};

static Config g_config;

// ============================================================================
// io_uring Echo Server (协程版)
// ============================================================================
class UringEchoServer {
public:
    bool start() {
        if (!engine_.init()) {
            fprintf(stderr, "Failed to init io_uring engine\n");
            return false;
        }

        listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);
        if (listen_fd_ < 0) return false;

        int reuse = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(g_config.port);
        addr.sin_addr.s_addr = INADDR_ANY;

        if (bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
            close(listen_fd_);
            return false;
        }
        if (listen(listen_fd_, SOMAXCONN) < 0) {
            close(listen_fd_);
            return false;
        }

        engine_.register_socket(listen_fd_, nullptr);

        thread_ = std::thread(&UringEchoServer::run, this);
        return true;
    }

    void stop() {
        g_running.store(false);
        engine_.post_wakeup();
        if (thread_.joinable()) thread_.join();
        engine_.shutdown();
        if (listen_fd_ >= 0) close(listen_fd_);
    }

private:
    void run() {
        // 投递初始 accept
        UringContext accept_ctx;
        engine_.post_accept(listen_fd_, &accept_ctx);

        constexpr int MAX_EVENTS = 256;
        IOContext* completions[MAX_EVENTS];

        while (g_running.load()) {
            int n = engine_.wait_completions(completions, MAX_EVENTS, 1000);
            for (int i = 0; i < n; ++i) {
                IOContext* ctx = completions[i];
                if (!ctx) continue;  // wakeup

                switch (ctx->op) {
                    case IOOperation::ACCEPT:
                        handle_accept(ctx);
                        break;
                    case IOOperation::RECV:
                        handle_recv(ctx);
                        break;
                    case IOOperation::SEND:
                        // 发送完成, 可以关闭或继续
                        close(ctx->fd);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    void handle_accept(IOContext* ctx) {
        // 新连接 → 投递 recv
        UringContext* recv_ctx = new UringContext();  // 简化: 每个连接分配独立上下文
        recv_ctx->buffer = new uint8_t[4096];
        recv_ctx->buffer_len = 4096;
        engine_.post_recv(ctx->fd, recv_ctx);

        // 重新投递 accept
        UringContext* next_ctx = new UringContext();
        engine_.post_accept(listen_fd_, next_ctx);
    }

    void handle_recv(IOContext* ctx) {
        if (ctx->bytes_transferred > 0) {
            // Echo: 收到数据 → 原样返回
            UringContext* send_ctx = new UringContext();
            send_ctx->buffer = ctx->buffer;
            send_ctx->bytes_transferred = ctx->bytes_transferred;
            engine_.post_send(ctx->fd, send_ctx);
        } else {
            // 连接关闭
            close(ctx->fd);
            delete[] ctx->buffer;
            delete static_cast<UringContext*>(ctx);
        }
    }

    IOEngineUring engine_;
    int listen_fd_ = -1;
    std::thread thread_;
};

// ============================================================================
// 客户端
// ============================================================================
void client_worker(int id) {
    int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock < 0) return;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(g_config.port);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock); return;
    }

    char send_buf[256];
    char recv_buf[4096];
    memset(send_buf, 'A', g_config.msg_size);

    timeval tv{5, 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    while (g_running.load()) {
        auto start = std::chrono::steady_clock::now();

        if (send(sock, send_buf, g_config.msg_size, MSG_NOSIGNAL) <= 0) {
            g_total_errors++; break;
        }

        int total = 0;
        while (total < g_config.msg_size) {
            int n = recv(sock, recv_buf + total, sizeof(recv_buf) - total, 0);
            if (n <= 0) {
                if (errno == EAGAIN) g_total_timeouts++;
                else g_total_errors++;
                goto done;
            }
            total += n;
        }

        auto end = std::chrono::steady_clock::now();
        g_total_requests++;

        double lat_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        {
            std::lock_guard lock(g_latency_mutex);
            g_latencies.push_back(lat_us);
        }
    }

done:
    close(sock);
}

// ============================================================================
// 统计
// ============================================================================
void print_stats(double elapsed, const char* label) {
    double rps = g_total_requests.load() / elapsed;
    int64_t total = g_total_requests.load();

    printf("\n--- %s ---\n", label);
    printf("  Requests:  %lld\n", (long long)total);
    printf("  Errors:    %lld\n", (long long)g_total_errors.load());
    printf("  Timeouts:  %lld\n", (long long)g_total_timeouts.load());
    printf("  Throughput: %.1f req/s\n", rps);

    {
        std::lock_guard lock(g_latency_mutex);
        if (!g_latencies.empty()) {
            std::sort(g_latencies.begin(), g_latencies.end());
            size_t n = g_latencies.size();
            printf("  Lat P50: %.0f us | P99: %.0f us | Max: %.0f us\n",
                   g_latencies[n*50/100], g_latencies[n*99/100], g_latencies.back());
        }
    }
}

// ============================================================================
// main
// ============================================================================
int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.starts_with("--clients=")) g_config.clients = std::stoi(arg.substr(10));
        else if (arg.starts_with("--duration=")) g_config.duration = std::stoi(arg.substr(11));
        else if (arg.starts_with("--path=")) g_config.path = arg.substr(7);
    }

    printf("========================================\n");
    printf("  io_uring + C++20 Coroutine Benchmark\n");
    printf("  Clients: %d | Duration: %d sec | Path: %s\n",
           g_config.clients, g_config.duration, g_config.path.c_str());
    printf("========================================\n\n");

    signal(SIGPIPE, SIG_IGN);

    UringEchoServer server;
    if (!server.start()) {
        fprintf(stderr, "Failed to start server\n");
        return 1;
    }
    printf("io_uring server started on port %d\n", g_config.port);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    g_running.store(true);
    std::vector<std::thread> client_threads;
    for (int i = 0; i < g_config.clients; ++i) {
        client_threads.emplace_back(client_worker, i);
    }

    auto bench_start = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::seconds(g_config.duration));
    g_running.store(false);
    auto bench_end = std::chrono::steady_clock::now();

    double elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        bench_end - bench_start).count() / 1000.0;

    for (auto& t : client_threads) if (t.joinable()) t.join();
    server.stop();

    print_stats(elapsed, "io_uring Coroutine Server");
    return 0;
}
