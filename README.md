# Cyrus-GW — 基于 C++20 与 io_uring 的流式 AI Gateway

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![Platform](https://img.shields.io/badge/Platform-Linux-blue.svg)](https://kernel.org)
[![Engine](https://img.shields.io/badge/Engine-io__uring-green.svg)](https://kernel.dk/io_uring.pdf)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Cyrus-GW** 是一个用 C++20 编写的高性能异步 HTTP 网关，专为 LLM 流式场景设计。
通过 **Gateway / Agent 双服务架构**，将接入层 I/O 与业务逻辑隔离；内置 **DeepSeek API** 集成，开箱即用。

> 仅支持 **Linux**。I/O 引擎在 `liburing` 可用时走完整 io_uring 实现，否则自动降级为 POSIX fallback（阻塞式 socket + 工作线程）。
>
> ⚠️ 两者的**功能**一致（接口、超时、协议处理相同），但**并发能力不同**：POSIX fallback 下 recv/send 在工作线程中同步阻塞，一条连接在收到数据前会占住一个 worker，因此并发上限约等于 `worker_threads`。它只用于无 liburing 时的功能验证；压测与生产请装 `liburing`（`apt install liburing-dev`）走 io_uring 路径。

---

## 目录

- [解决了什么问题](#解决了什么问题)
- [架构设计](#架构设计)
- [特性](#特性)
- [快速开始](#快速开始)
- [HTTP API](#http-api)
- [配置说明](#配置说明)
- [项目结构](#项目结构)
- [构建目标](#构建目标)
- [测试](#测试)
- [基准测试](#基准测试)
- [技术栈](#技术栈)
- [已知限制](#已知限制)
- [License](#license)

---

## 解决了什么问题

LLM 流式场景的接入层有几个容易踩的坑，这个项目逐个处理：

**1. 一条流式请求把 I/O 线程占住，其他连接排不上队**
SSE 是长连接，一个回答可能持续几十秒。若在 I/O 线程里同步等待上游，线程就被这一条连接独占。本项目把流式请求转交到**有界的中继线程池**（默认 256 并发），I/O worker 立即返回继续服务其他连接；并发超限时快速失败返回 503，而不是无限排队把内存撑爆。

**2. token 级实时性：用户不该等整段回答生成完才看到字**
收到的每个上游数据块立即 `send` 给客户端（`send_raw_sync`），不做整包缓冲，首字节延迟最小。响应头一旦发出，后续错误就只能走 SSE 通道——所以统一用 OpenAI 兼容的 `event: error` 传递，客户端不会收到一个"半截就没有了"的流。

**3. HTTP 半包 / 粘包导致的 body 截断与请求错位**
TCP 不保留消息边界：一次 `recv` 可能只有半个请求，也可能粘着下一个请求。请求体严格按 `Content-Length`（或分块长度）读满才判定完成，半包时保持状态等待续收，不会"只读到 header 就当作请求结束"；一次 `recv` 中多出来的字节作为流水线数据保留，不丢弃、不串包；分块编码的终止 `\r\n` 也消费干净，不留进下一轮的缓冲。

**4. 上游慢、卡住、或者悄悄断开，请求就悬在那里**
三层超时（首包 15s / 总 120s / 帧间空闲 30s）+ 连接探活（非阻塞 `MSG_PEEK` 识别对端已关闭）+ 断线自动重连。连接池里"看起来还活着、实际对端已关闭"的连接会被识别出来重建，而不是反复派发出去让请求间歇性失败。

**5. 高并发下的资源耗尽**
连接数上限（超限直接拒）、keep-alive 空闲连接定时回收、连接对象随用随还（不随请求数增长）、预分配缓冲池 RAII 管理（稳态零 malloc）、令牌桶两级限流（全局 + 按客户端 IP）。

**6. 服务关不掉 / 关不干净**
优雅关闭要处理"worker 正阻塞在 recv 上""中继线程还持有连接指针""监听线程还阻塞在 accept 上"这几种情况，否则进程会卡在 `join` 上只能被 SIGKILL。关闭顺序是：停止 accept → 排空中继线程 → 唤醒并回收 worker → 关闭引擎，实测流式进行中收到 SIGTERM 也能干净退出。

**7. 部署依赖重**
除 `pthread` 与可选 `liburing` 外零第三方库依赖；调用 DeepSeek 走 `curl` 子进程而非引入 HTTP 客户端库；没有 `liburing` 时自动降级为 POSIX fallback，功能不变（并发上限见上方说明）。

---

## 架构设计

### Gateway / Agent 双服务隔离

解决 LLM 调用耗时长、流式连接持续时间长、错误链路难定位三大问题：**将接入层 I/O 与业务逻辑部署为独立进程，故障不传染，瓶颈可独立定位。**

```
                          ┌─────────────────────────────┐
     HTTP Clients ───────→│      Cyrus-GW Gateway       │
                          │   (io_uring + 工作线程 Proactor) │
                          │                             │
                          │  · 连接接入 (accept)         │
                          │  · HTTP/1.1 解析 (状态机)     │
                          │  · Token Bucket 限流          │
                          │  · SSE 流式透传 (中继线程池)  │
                          └──────────┬──────────────────┘
                                     │ TCP (二进制帧协议)
                          ┌──────────▼──────────────────┐
                          │        Cyrus Agent          │
                          │     (select 事件循环)        │
                          │                             │
                          │  · LLMProvider 策略模式      │
                          │  · DeepSeek API (真实 LLM)  │
                          │  · Mock LLM (本地演示)       │
                          │  · 工具路由                  │
                          └─────────────────────────────┘
```

| 层 | 进程 | I/O 模型 | 职责 |
|----|------|----------|------|
| **接入层** | Gateway | io_uring / POSIX fallback + 工作线程 Proactor | 连接接入、HTTP 解析、限流、SSE 透传 |
| **业务层** | Agent | `select()` 事件循环 | LLMProvider 策略、LLM 调用、工具路由 |

### Gateway I/O 模型（Proactor）

Gateway 采用 **Proactor 模式**：由 I/O 引擎投递异步操作（`post_accept` / `post_recv` / `post_send`），
工作线程在 `wait_completions()` 上等待完成事件并分发回调。

- **io_uring 路径**（`CYRUS_HAS_LIBURING=1`）：内核级异步 I/O，`SQE`/`CQE` 提交/收割。
- **POSIX fallback**（`CYRUS_HAS_LIBURING=0`）：独立 accept 线程 + 工作线程内同步 `recv`/`send`，完成事件经 `completion_queue` + `condition_variable` 通知。

SSE 长连接通过**有界中继线程池**（`dispatch_relay`/`relay_stream`）解耦，避免占用 I/O worker 线程；
关闭时优雅排空（`drain_relays`）保证 `Connection`/`AgentClient` 生命周期安全。

### C++20 协程层（已接通，非骨架）

`coro_engine.hpp` 提供 `Task<T>` / `co_await` 接口，**完成事件 → 协程恢复的链路是通的**，
由 `tests/test_coro.cpp` 覆盖（ASAN 下同样通过）：

```
co_await async_recv(fd, buf)
   │  await_suspend: 填充 ctx → post_recv() 投递 SQE, 协程挂起
   │  引擎把 ctx 指针编码进 sqe->user_data (高 8 位操作类型 + 低 56 位指针)
   ▼
wait_completions() 收割 CQE → 解码出 ctx → 填充 bytes_transferred/error
   │  发现 ctx->on_complete 非空 → 调用回调
   ▼
回调经 ctx->user_data 找回 awaiter → continuation.resume() → 从 await_resume() 继续
```

**恢复发生在收割该 CQE 的那个 I/O 线程上，不做跨线程投递**：省掉每次 I/O 的入队 + 唤醒开销，
协程帧也只被一个线程访问、无需加锁；代价是协程体不能长时间占 CPU（否则会拖住该 worker 收割其他
完成事件），所以协程要在每个 I/O 边界让出。

已处理的坑（都在测试里有对应断言）：
- **协程帧生命周期**：`detach()` 的 fire-and-forget 任务在 `final_suspend` 自毁，不泄漏帧；
  「帧已销毁」由帧上局部对象的析构标志验证。
- **对称转移**：`co_await Task` 通过 `await_suspend` 返回子协程句柄，而不是在 `await_suspend`
  里 `resume()` —— 后者会让父协程在子协程的栈上继续执行，父协程一销毁就悬垂。
- **投递失败不能挂死**：`post_recv/post_send` 失败时立刻恢复协程（`h.resume()` 必须是最后一条语句，
  恢复后帧可能已销毁）。
- **POSIX 分支的锁**：完成回调必须在 `completion_mutex_` 之外触发，否则恢复的协程紧接着
  `post_recv` 重新取同一把锁会自死锁。

`coro_session.hpp` 是基于它的连接处理参考实现（收 → 解析 → 应答，半包保持状态、粘包字节不丢），
与 `Connection` 是同一件事的协程写法。

> 现状：协程链路已接通并有测试覆盖，但**网关主链路仍走 `Connection`（线程 + 完成事件分发）**。
> 把连接处理切到协程需要先在装了 `liburing` 的环境做等价性验证 —— 无 liburing 时引擎走 POSIX
> fallback，`post_recv` 是同步阻塞实现，协程退化为「阻塞 + 挂起」，验证不出真实异步行为。

### Gateway ↔ Agent 二进制帧协议

Gateway 与 Agent 之间不经过 HTTP，而是使用紧凑的二进制协议：

```
+----------------+------------------+------------------+
| 4 字节长度前缀  |  MessageHeader    |   Payload         |
| (网络字节序)     |  (16 字节)        |   (变长)          |
+----------------+------------------+------------------+
```

每条连接串行收发（协议非多路复用）。Gateway 通过 `AgentClient::try_acquire()`（CAS 忙标记）保证同一 socket 不被并发流复用。

### LLMProvider 策略模式

Agent 通过 `LLMProvider` 抽象接口支持可插拔 LLM 后端：

| Provider | 说明 |
|----------|------|
| `DeepSeekProvider` | 通过 `popen("curl ...")` 调用 DeepSeek API（HTTPS + SSE），解析流式 `delta.content` |
| `MockLLMProvider` | 内置模拟 LLM，返回预设 tokens，无需网络 |
| *自定义* | 实现 `LLMProvider::generate()` 即可接入任意 LLM |

```
ChatHandler.handle()
  │
  └─→ provider_->generate(prompt)
        │
        ├─ DeepSeekProvider → curl → api.deepseek.com/v1/chat/completions
        │                       └─ 逐行解析 SSE "data:" → 提取 delta.content
        │
        └─ MockLLMProvider  → 本地生成 tokens
```

---

## 特性

- **DeepSeek API 集成**：开箱即用，API Key 从环境变量 `DEEPSEEK_API_KEY` 读取（绝不硬编码）
- **可插拔 LLMProvider**：策略模式，对接任意 LLM 只需实现一个接口
- **io_uring 异步 I/O**：内核级异步 I/O，无 `liburing` 时自动降级 POSIX fallback
- **HTTP/1.1 解析**：逐字节有限状态机，支持 keep-alive、Content-Length、chunked TE
- **半包 / 粘包健壮处理**：请求体按 `Content-Length`（或分块长度）读满才判定完成，半包保持状态等待续收，不会「只读到 header 就当作请求结束」而截断 body；一次 `recv` 中多出来的字节作为流水线数据保留（`pending_data_`），不丢弃、不串包，消除后续请求错位；客户端提前断开时识别并记录 body 截断
- **二进制帧协议**：4 字节长度前缀 + 流式解码器（`ProtocolDecoder`），Gateway↔Agent 侧同样正确处理 TCP 拆包/粘包
- **SSE 实时流式透传**：ChunkedDecoder 剥离传输编码 → 逐 token 推送，首字节延迟最低
- **中继与 I/O 线程解耦**：流式请求交给独立中继线程池，不长期占用 I/O worker；并发有界（默认 256），超限直接 503 快速失败而非排队堆积
- **三层超时保护**：首包超时(15s) / 总超时(120s) / 帧间空闲(30s)，SSE header 后统一 `event: error`
- **OpenAI 兼容**：`POST /v1/chat/completions` 端点，标准 SSE 格式
- **Agent 连接池**：round-robin 调度 + CAS 忙标记（串行协议独占，避免并发复用同一 socket）+ 断线自动重连 + 周期性健康检查
- **Token Bucket 限流**：全局 + 每 IP 双重限流
- **缓冲池**：预分配 RAII 管理，零 malloc 开销
- **优雅关闭**：异步信号安全处理 + SSE 中继排空 + 引擎正确关停（含 POSIX 下 accept/recv 阻塞解除）

---

## 快速开始

### 构建

依赖：**CMake ≥ 3.21**、**GCC/Clang（支持 C++20）**、`liburing`（可选，缺失则走 POSIX fallback）、`curl`（Agent 调用 DeepSeek 时使用）。

```bash
git clone <repo-url> && cd Cyrus-GW

# 方式一：手动配置
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j

# 方式二：CMake Presets
cmake --preset debug          # 生成 build/debug
cmake --build --preset debug
```

### 运行

**终端 1 — Agent（LLM 业务层）**

```bash
# 未设置 DEEPSEEK_API_KEY → 自动回退 Mock provider（无需网络）
./build/cyrus_agent/cyrus_agent --port 9999

# 使用真实 DeepSeek LLM
export DEEPSEEK_API_KEY="sk-..."
./build/cyrus_agent/cyrus_agent --port 9999
```

**终端 2 — Gateway（HTTP 接入层）**

```bash
./build/cyrus_gateway/cyrus_gateway config/gateway.conf
```

### 验证

```bash
# 健康检查
curl http://localhost:8080/health

# 欢迎页面
curl http://localhost:8080/

# SSE 流式聊天（DeepSeek 或 Mock，取决于 Agent 启动方式）
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"你好"}]}'
# → 逐 token 实时流式输出... 以 data: [DONE] 结束
```

### 停止

对两个进程发送 `SIGINT`（Ctrl+C）即触发优雅关闭。也可用端口定位进程：

```bash
fuser -k 8080/tcp   # Gateway
fuser -k 9999/tcp   # Agent
```

---

## HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/` | 欢迎页面 |
| `GET` | `/health` | 健康检查（含实时指标，见下） |
| `POST` | `/v1/chat/completions` | Chat Completion（SSE 流式，OpenAI 兼容） |
| `POST` | `/v1/chat` | 同上（别名路由） |

### `/health` 响应字段

```json
{
  "status": "ok",
  "version": "2.0.0",
  "platform": "Linux",
  "engine": "io_uring(POSIX)",
  "uptime_seconds": 123,
  "active_relays": 2,
  "agents": { "healthy": 4, "total": 4 }
}
```

| 字段 | 含义 |
|------|------|
| `engine` | `io_uring`（完整实现）或 `io_uring(POSIX)`（fallback） |
| `uptime_seconds` | 进程运行秒数 |
| `active_relays` | 当前活跃的 SSE 流式中继线程数 |
| `agents` | Agent 连接池健康数 / 总数 |

---

## 配置说明

编辑 `config/gateway.conf`（INI 风格，`#` 或 `;` 开头为注释）：

```ini
[server]
listen_address = 0.0.0.0          # 监听地址
listen_port = 8080                # 监听端口
worker_threads = 0                # 工作线程数，0 = 自动（CPU 核心数）

[agent]
host = 127.0.0.1                  # Agent 后端地址
port = 9999                       # Agent 后端端口
pool_size = 4                     # Agent 连接池大小（同时限制并发流数）
connect_timeout_ms = 5000         # 连接超时（非阻塞 connect + poll）
request_timeout_ms = 30000        # 一次 Agent 请求的总预算，[sse] total_timeout_ms 优先

[connection]
max_connections = 10000           # 最大并发连接数（超限直接拒绝）
keepalive_timeout_s = 5           # Keep-alive 空闲超时（秒），由扫描线程回收
max_keepalive_requests = 1000     # 单连接最大 keep-alive 请求数
max_header_size = 8192            # 最大请求头大小（字节），超限返回 400

[logging]
level = INFO                      # DEBUG | INFO | WARN | ERROR
# 输出到 stdout；WARN 及以上立即刷盘，INFO/DEBUG 保持缓冲

[rate_limit]
global_rate = 1000                # 全局限流 tokens/秒
global_capacity = 2000            # 全局突发容量
per_ip_rate = 50                  # 每 IP 限流 tokens/秒（按 accept 时提取的客户端 IP）
per_ip_capacity = 100             # 每 IP 突发容量

[sse]
first_byte_timeout_ms = 15000     # 首包超时
total_timeout_ms = 120000         # 总超时
idle_timeout_ms = 30000           # 帧间空闲超时
```

> 上表所有配置项均已接线生效。
---

## 项目结构

```
Cyrus-GW/
├── CMakeLists.txt
├── CMakePresets.json              # debug / release 预设
├── README.md
├── config/
│   └── gateway.conf
├── cmake/
│   └── CompilerWarnings.cmake     # 统一编译警告
├── cyrus_common/                  # 静态库
│   ├── include/cyrus/
│   │   ├── types.hpp              # 核心类型、错误码
│   │   ├── buffer_pool.hpp        # 缓冲池 (RAII)
│   │   ├── chunked_decoder.hpp    # Chunked TE 解码器
│   │   ├── protocol.hpp           # 二进制帧协议
│   │   ├── sse_codec.hpp          # SSE 格式化
│   │   ├── token_bucket.hpp       # 限流器
│   │   ├── config.hpp             # 配置解析
│   │   ├── logger.hpp             # 日志系统
│   │   └── platform.hpp           # 平台抽象 (Linux)
│   └── src/
│       ├── platform_linux.cpp     # Linux 信号处理 (SIGINT/SIGTERM/SIGPIPE)
│       └── protocol.cpp
├── cyrus_gateway/                 # Gateway 可执行文件
│   ├── include/cyrus/gateway/
│   │   ├── server.hpp             # 服务器主控
│   │   ├── connection.hpp         # 连接状态机
│   │   ├── http_parser.hpp        # HTTP 解析器 (FSM)
│   │   ├── router.hpp             # 路由器 + Agent 连接池 + SSE 中继线程池
│   │   ├── sse_handler.hpp        # SSE 中继 + 三层超时
│   │   ├── io_engine.hpp          # I/O 引擎抽象
│   │   ├── io_engine_uring.hpp    # io_uring 引擎 (POSIX fallback)
│   │   ├── coro_engine.hpp        # C++20 协程适配层 (Task/Awaitable, 已接通)
│   │   ├── coro_session.hpp       # 协程版连接处理 (参考实现)
│   │   └── agent_client.hpp       # Agent TCP 客户端 (CAS 忙标记)
│   └── src/
│       ├── main.cpp
│       ├── server.cpp
│       ├── connection.cpp
│       ├── http_parser.cpp
│       ├── router.cpp
│       ├── sse_handler.cpp
│       ├── io_engine_uring.cpp
│       └── agent_client.cpp
├── cyrus_agent/                   # Agent 可执行文件
│   ├── include/cyrus/agent/
│   │   ├── agent_server.hpp       # select() 事件循环
│   │   ├── request_handler.hpp    # RequestHandler + LLMProvider 抽象
│   │   ├── chat_handler.hpp       # ChatHandler + MockLLMProvider
│   │   ├── deepseek_provider.hpp  # DeepSeek API Provider (curl)
│   │   └── echo_handler.hpp       # Echo 调试处理器
│   └── src/
│       ├── main.cpp
│       └── agent_server.cpp
├── tests/                         # 测试（9 单元 + 1 集成）
└── benchmark/                     # 基准测试
    ├── bench_epoll.cpp            # epoll Reactor 基线
    ├── bench_uring.cpp            # io_uring + C++20 协程版
    ├── run_bench.sh               # 编排脚本（输出 CSV + 摘要）
    ├── wrk_post.lua               # wrk POST 脚本
    └── bench_*.json               # 历史基准结果
```

---

## 构建目标

| 目标 | 类型 | 说明 |
|------|------|------|
| `cyrus_common` | 静态库 | 基础类型、日志、配置、缓冲池、协议、chunked 解码器 |
| `cyrus_gateway` | 可执行文件 | HTTP 网关（io_uring / POSIX fallback） |
| `cyrus_agent` | 可执行文件 | Agent 服务（select 事件循环 + LLMProvider 策略） |
| `test_http_parser` | 测试 | HTTP 解析器 |
| `test_body_parser` | 测试 | Body 截断 + 粘包回归 |
| `test_chunked_decoder` | 测试 | Chunked 解码器 |
| `test_protocol` | 测试 | 协议编解码 |
| `test_sse` | 测试 | SSE 编解码 |
| `test_sse_error` | 测试 | SSE 错误处理 |
| `test_buffer` | 测试 | 缓冲池 |
| `test_token_bucket` | 测试 | Token Bucket 限流 |
| `test_coro` | 测试 | 协程 ↔ 完成事件链路（resume、帧生命周期、半包） |
| `integration_test` | 集成测试 | 启动 Agent + Gateway，curl 验证 `/health` 与 SSE 流 |
| `cyrus_bench_epoll` | 基准测试 | epoll Reactor 基线 |
| `cyrus_bench_uring` | 基准测试 | io_uring + 协程版（需 liburing） |

---

## 测试

```bash
# 构建并运行全部测试（9 单元 + 1 集成）
cmake --build build -j
ctest --test-dir build --output-on-failure

# 单独运行某个单元测试
./build/tests/test_http_parser

# 单独运行集成测试（起 Agent + Gateway，curl 验证）
./build/tests/integration_test.sh ./build/cyrus_agent/cyrus_agent ./build/cyrus_gateway/cyrus_gateway
```

集成测试会启动 Agent（自动回退 Mock provider）与 Gateway，验证：

1. `GET /health` 返回 `"status":"ok"`；
2. `POST /v1/chat/completions` 返回完整 SSE 流并以 `data: [DONE]` 结束；
3. `POST /v1/chat` 别名路由同样返回完整 SSE 流。

测试端口可用环境变量覆盖：`CYRUS_TEST_AGENT_PORT`（默认 19999）、`CYRUS_TEST_GATEWAY_PORT`（默认 18080）。

---

## 基准测试

对比 **epoll Reactor** 与 **io_uring** 两种并发模型（两者的 echo server 都是单线程事件循环，可直接对照）：

```bash
./build/uring/benchmark/cyrus_bench_epoll  --clients=500 --duration=8
./build/uring/benchmark/cyrus_bench_uring  --clients=500 --duration=8
```

**实测（本机，echo 负载，8 秒，单次测量）：**

| 并发 | epoll | io_uring |
|------|-------|----------|
| c=100 | 185,011 req / 37.0k RPS / P99 3.9ms | 176,774 req / 35.4k RPS / P99 4.3ms |
| c=500 | 279,487 req / 34.9k RPS / P99 20.7ms | 253,694 req / 31.7k RPS / P99 23.3ms |

两个引擎在这台机器上**基本持平**（io_uring 略低 5%~10%）。注意这是 echo 负载，不含 Agent / LLM 环节。

> ⚠️ 早期文档里「c=500 时 io_uring 完成请求数是 epoll 的 2.15×」的说法**目前无法复现**：既没有当时的脚本，也没有留下原始数据。另外 `--path=pure_error|agent_single|full_chain` 三个取值当前**不改变实际负载**（只影响打印），因此那三行数据等价，不能用于瓶颈定位。需要真实链路对比时，应先把 `--path` 实现成不同工作负载（或直接用 wrk 打真实网关）。

---

## 已知限制

1. **POSIX fallback 的并发上限 ≈ `worker_threads`**（已在开头说明）：无 liburing 时 recv/send 在工作线程里同步阻塞，一条连接会占住一个 worker。压测请装 liburing。
2. **连接对象运行期不回收**：只关闭 socket 与缓冲，`Connection` 对象保留到关机统一销毁。原因是"无人引用"难以可证明地判定 —— worker（完成事件）、SSE 中继线程、空闲扫描线程三方都可能持有裸指针，而 `ctx->user_data` 指向对象、完成事件可能在任意时刻到达。试过三种销毁方案（worker 侧摘除 / 中继侧摘除 / 扫描线程统一摘除 + 在途计数），都在压力下出现 use-after-free（ASAN 实测），因此改为保守策略。代价是对象常驻内存（约百余字节/连接）。彻底解决需要换所有权模型（每连接单一所有者线程，或让引用计数句柄随完成事件传递）。
3. **协程仍是并行路径，不是主链路**：`coro_engine.hpp` / `coro_session.hpp` 已接通并有测试覆盖（含真 io_uring），但网关连接处理仍走 `Connection`（线程 + 完成事件分发）。
4. **基准的 `--path` 不改变负载**（见上）。
5. **`io_uring` 引擎的 SQ/CQ 用一把互斥锁串行化**：liburing 的 `io_uring_get_sqe()`/`io_uring_submit()` 不是线程安全的，多 worker 共享一个 ring 必须序列化，否则 SQE 会被覆盖、完成事件与操作错配（实测表现为 accept 完成里解析出别的操作）。更彻底的做法是每线程独立 ring（`IORING_SETUP_SQPOLL`/`ATTACH_WQ`）。

---

## 技术栈

| 技术 | 用途 |
|------|------|
| C++20 | 核心语言（`std::format`、`std::source_location`、concepts） |
| io_uring | Linux 异步 I/O 引擎（无 liburing 时 POSIX fallback） |
| C++20 Coroutines | 协程适配层 `Task<T>`/`Awaitable`（链路已接通，测试覆盖） |
| CMake + Ninja/Make | 构建系统 |
| SSE | 流式推送协议（OpenAI 兼容） |
| TCP | Gateway ↔ Agent 二进制帧协议 |
| DeepSeek API | 内置 LLM Provider（HTTPS + SSE，经 curl） |

---

## License

MIT License. See [LICENSE](LICENSE) for details.
