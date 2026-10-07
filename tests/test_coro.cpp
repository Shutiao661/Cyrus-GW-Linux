// ============================================================================
// test_coro.cpp - 协程 ↔ io_uring 完成事件链路测试
// ============================================================================
// 验证三件事:
//   1. 完成事件 → awaiter 回调 → 协程 resume 的完整链路
//      (在 socketpair 上跑真实的 http_session: 收 → 解析 → 应答)
//   2. 协程帧生命周期: detach 的任务跑完后帧自毁, 不泄漏
//   3. co_await 子协程 (continuation) 与 co_await Task<T> 的返回值
//
// 关于运行环境: 本机没有 liburing 时引擎走 POSIX fallback, 其 post_recv/
// post_send 是同步阻塞实现, 协程退化为"阻塞 + 挂起"。但分发与恢复走的是
// 同一条代码路径 (wait_completions → ctx->on_complete → continuation.resume),
// 因此这里验证的是真实的恢复链路, 只是没有异步收益。
// ============================================================================

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>

#include <sys/socket.h>
#include <unistd.h>

#include "../cyrus_gateway/include/cyrus/gateway/coro_session.hpp"
#include "../cyrus_gateway/include/cyrus/gateway/io_engine_uring.hpp"

using namespace cyrus;
using namespace cyrus::gateway;
using namespace cyrus::gateway::coro;

static int tests_passed = 0, tests_failed = 0;

#define TEST(name) printf("  TEST: %s ... ", name);
#define PASS() do { printf("PASSED\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("FAILED: %s\n", msg); tests_failed++; } while(0)
#define CHECK(cond, msg) do { if (!(cond)) { FAIL(msg); return; } } while(0)

// ============================================================================
// 帧销毁探测器: 作为协程帧上的局部对象, 帧销毁时析构 → 置位标志。
// 用它证明 detached 的任务没有泄漏协程帧。
// ============================================================================
struct FrameWatch {
    std::atomic<bool>* flag;
    ~FrameWatch() { flag->store(true, std::memory_order_release); }
};

// 包一层再 co_await 真正的 session: 同时验证 continuation 链路与帧生命周期
Task<void> watched_session(IOEngineUring& engine, socket_t fd, BufferPool& pool,
                           std::atomic<bool>* frame_gone) {
    FrameWatch watch{frame_gone};
    co_await http_session(engine, fd, pool);
}

// 驱动引擎直到条件满足或超时。
// 这里是"事件循环"的位置: wait_completions 收割 CQE, 内部回调 resume 协程。
static bool drive_until(IOEngineUring& engine, const std::atomic<bool>& done,
                        int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    IOContext* ctxs[16];
    while (!done.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        engine.wait_completions(ctxs, 16, 50);
    }
    return true;
}

// ============================================================================
// 1. 端到端: 协程 session 处理一个完整 HTTP 请求
// ============================================================================
void test_coro_http_session() {
    TEST("coroutine session end-to-end (CQE -> resume)");

    IOEngineUring engine;
    engine.init();
    BufferPool pool(64, 4096);

    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
        FAIL("socketpair failed");
        return;
    }

    std::string response;
    std::thread client([&] {
        const char* req = "GET /health HTTP/1.1\r\nHost: cyrus\r\n\r\n";
        ::send(sv[1], req, std::strlen(req), 0);
        char buf[1024];
        ssize_t n;
        while ((n = ::recv(sv[1], buf, sizeof(buf), 0)) > 0) {
            response.append(buf, static_cast<size_t>(n));
        }
        ::close(sv[1]);
    });

    std::atomic<bool> frame_gone{false};
    auto session = watched_session(engine, sv[0], pool, &frame_gone);
    session.detach();   // fire-and-forget: 帧在结束时自毁

    const bool completed = drive_until(engine, frame_gone, 5000);
    client.join();

    if (!completed) { FAIL("coroutine did not finish (timeout)"); return; }
    if (response.rfind("HTTP/1.1 200 OK", 0) != 0) { FAIL("no 200 response"); return; }
    if (response.find("\"status\":\"ok\"") == std::string::npos) {
        FAIL("response body mismatch");
        return;
    }
    PASS();
}

// ============================================================================
// 2. 半包: 请求分两次写入, 协程应保持解析状态等待续收
// ============================================================================
void test_coro_half_packet() {
    TEST("coroutine session handles split request (half packet)");

    IOEngineUring engine;
    engine.init();
    BufferPool pool(64, 4096);

    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
        FAIL("socketpair failed");
        return;
    }

    std::string response;
    std::thread client([&] {
        const char* part1 = "GET /health HTTP/1.1\r\nHost: cy";
        const char* part2 = "rus\r\n\r\n";
        ::send(sv[1], part1, std::strlen(part1), 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        ::send(sv[1], part2, std::strlen(part2), 0);

        char buf[1024];
        ssize_t n;
        while ((n = ::recv(sv[1], buf, sizeof(buf), 0)) > 0) {
            response.append(buf, static_cast<size_t>(n));
        }
        ::close(sv[1]);
    });

    std::atomic<bool> frame_gone{false};
    auto session = watched_session(engine, sv[0], pool, &frame_gone);
    session.detach();

    const bool completed = drive_until(engine, frame_gone, 5000);
    client.join();

    if (!completed) { FAIL("coroutine did not finish (timeout)"); return; }
    if (response.rfind("HTTP/1.1 200 OK", 0) != 0) {
        FAIL("split request not served");
        return;
    }
    PASS();
}

// ============================================================================
// 3. Task<T> 返回值 + 父子协程 continuation (无需引擎)
// ============================================================================
Task<int> compute_answer() { co_return 42; }

Task<void> parent_task(std::atomic<bool>* frame_gone, int* out) {
    FrameWatch watch{frame_gone};
    *out = co_await compute_answer();
}

void test_task_value_and_continuation() {
    TEST("Task<T> value via continuation (no I/O)");

    std::atomic<bool> frame_gone{false};
    int value = 0;

    auto task = parent_task(&frame_gone, &value);
    task.detach();   // 同步跑完: 帧在 final_suspend 自毁

    CHECK(frame_gone.load(std::memory_order_acquire),
          "detached task frame destroyed");
    CHECK(value == 42, "co_await Task<int> returned the value");
    PASS();
}

// ============================================================================
// 4. I/O 错误必须唤醒协程而不是让它永远挂着:
//    用一个非法 fd, post_recv 会以 error=EBADF 完成 →
//    await_resume 返回 0 → session 走完并销毁帧。
// ============================================================================
void test_io_error_resumes() {
    TEST("I/O error completion resumes the coroutine (no hang)");

    IOEngineUring engine;
    engine.init();
    BufferPool pool(64, 4096);

    std::atomic<bool> frame_gone{false};
    auto session = watched_session(engine, /*fd=*/-1, pool, &frame_gone);
    session.detach();

    const bool completed = drive_until(engine, frame_gone, 2000);
    CHECK(completed, "coroutine finished instead of hanging on a failed post");
    PASS();
}

int main() {
    printf("========================================\n");
    printf("  test_coro - coroutine <-> engine\n");
    printf("========================================\n");

    test_coro_http_session();
    test_coro_half_packet();
    test_task_value_and_continuation();
    test_io_error_resumes();

    printf("\n========================================\n");
    printf("  Results: %d passed, %d failed\n", tests_passed, tests_failed);
    printf("========================================\n");
    return tests_failed > 0 ? 1 : 0;
}
