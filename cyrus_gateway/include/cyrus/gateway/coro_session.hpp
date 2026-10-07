// ============================================================================
// coro_session.hpp - 协程版连接处理 (参考实现)
// ============================================================================
// 与 connection.cpp 是同一件事的两种写法:
//   - Connection: 线程 + 完成事件分发 (callback/状态机风格)
//   - http_session: 协程, 一次 co_await 一次 I/O (顺序代码风格)
//
// 用途: 作为把连接处理切换到协程的落点。当前由 tests/test_coro.cpp 驱动,
// 验证"完成事件 → 协程恢复"的完整链路; 接入 Server 的 accept 路径需要
// 在 io_uring 环境下先做等价性验证 (见 README「协程层」一节)。
// ============================================================================

#pragma once

#include "coro_engine.hpp"
#include "http_parser.hpp"
#include "cyrus/buffer_pool.hpp"
#include "cyrus/logger.hpp"

#include <string>
#include <vector>

namespace cyrus {
namespace gateway {
namespace coro {

// ============================================================================
// send_all() - 写满整段数据
// ============================================================================
// 以 `co_await send_all(...)` 的形式被调用: 这里 co_await 的是一个 Task<void>,
// 顺带验证"父协程被 continuation 恢复"这条链路 (而不只是 awaiter 的 CQE 回调)。
Task<void> send_all(IOEngineUring& engine, socket_t fd, std::string_view data) {
    size_t sent = 0;
    while (sent < data.size()) {
        size_t n = co_await async_send(
            &engine, fd,
            reinterpret_cast<const uint8_t*>(data.data() + sent),
            data.size() - sent);
        if (n == 0) {
            co_return;  // 对端关闭或出错
        }
        sent += n;
    }
}

// ============================================================================
// http_session() - 一条连接的协程处理循环
// ============================================================================
// 收 → 喂解析器 → (完整)应答 → 关闭。半包时解析器保持状态, 继续 co_await
// 下一批数据; 一次收到的多余字节留在 pending 里, 不被丢弃。
// 响应固定为 Connection: close, 因此一个请求处理完即结束会话。
Task<void> http_session(IOEngineUring& engine, socket_t fd, BufferPool& pool) {
    HttpParser parser;

    auto in = pool.acquire();
    if (!in.valid()) {
        cyrus_close_socket(fd);
        co_return;
    }

    // 尚未解析完的字节 (半包残留 / 粘包多出来的部分)
    std::vector<uint8_t> pending;

    for (;;) {
        size_t n = co_await async_recv(&engine, fd, in.data(), in.capacity());
        if (n == 0) {
            break;  // EOF 或错误
        }
        in.set_length(n);
        pending.insert(pending.end(), in.data(), in.data() + n);

        size_t consumed = parser.parse(pending.data(), pending.size());
        if (parser.is_error()) {
            LOG_DEBUG("coro session: parse error on fd={}", static_cast<int>(fd));
            break;
        }

        if (parser.is_complete()) {
            static constexpr std::string_view kBody = "{\"status\":\"ok\"}\n";
            std::string resp;
            resp.reserve(192);
            resp += "HTTP/1.1 200 OK\r\n";
            resp += "Server: Cyrus-GW/2.0 (coro)\r\n";
            resp += "Content-Type: application/json\r\n";
            resp += "Content-Length: ";
            resp += std::to_string(kBody.size());
            resp += "\r\nConnection: close\r\n\r\n";
            resp += kBody;

            LOG_DEBUG("coro session: {} {} -> 200 (fd={})",
                      http_method_to_sv(parser.result().method),
                      parser.result().uri,
                      static_cast<int>(fd));

            co_await send_all(engine, fd, resp);
            break;  // Connection: close
        }

        // 丢弃已消费字节, 保留未消费的 (粘包/流水线残留)
        if (consumed > 0 && consumed <= pending.size()) {
            pending.erase(pending.begin(),
                          pending.begin() + static_cast<ptrdiff_t>(consumed));
        }
    }

    cyrus_close_socket(fd);
}

} // namespace coro
} // namespace gateway
} // namespace cyrus
