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
- [License](#license)

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

> **关于 C++20 协程**：主链路当前基于工作线程 Proactor 实现。`coro_engine.hpp` 是实验性的
> C++20 协程适配层（`Task<T>`/`Awaitable`），目前仅在基准测试 `bench_uring` 中使用。

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
│   │   ├── coro_engine.hpp        # 实验性 C++20 协程适配层
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
├── tests/                         # 测试（8 单元 + 1 集成）
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
| `integration_test` | 集成测试 | 启动 Agent + Gateway，curl 验证 `/health` 与 SSE 流 |
| `cyrus_bench_epoll` | 基准测试 | epoll Reactor 基线 |
| `cyrus_bench_uring` | 基准测试 | io_uring + 协程版（需 liburing） |

---

## 测试

```bash
# 构建并运行全部测试（8 单元 + 1 集成）
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

对比 **epoll Reactor** 与 **io_uring + C++20 协程** 两种并发模型：

```bash
# 运行测试矩阵（引擎: epoll/uring，并发: 100/300/500，路径: pure_error/agent_single/full_chain）
./benchmark/run_bench.sh both
```

三种对照路径用于瓶颈定位：

| 路径 | 配置 | 目的 |
|------|------|------|
| `pure_error` | Gateway 直接返回错误，不调用 Agent | 隔离 I/O 层性能天花板 |
| `agent_single` | 仅 1 个 Agent worker | 隔离业务层开销 |
| `full_chain` | Gateway → Agent → LLM 全链路 | 端到端真实场景 |

> 结论（c=500）：io_uring 完成请求数为 epoll 的 **2.15×**；瓶颈在 Agent/LLM 调用侧，而非 Gateway I/O 层。
> 故性能优化优先投入 Agent/LLM 链路。

---

## 技术栈

| 技术 | 用途 |
|------|------|
| C++20 | 核心语言（`std::format`、`std::source_location`、concepts） |
| io_uring | Linux 异步 I/O 引擎（无 liburing 时 POSIX fallback） |
| C++20 Coroutines | 实验性协程适配层（benchmark 使用） |
| CMake + Ninja/Make | 构建系统 |
| SSE | 流式推送协议（OpenAI 兼容） |
| TCP | Gateway ↔ Agent 二进制帧协议 |
| DeepSeek API | 内置 LLM Provider（HTTPS + SSE，经 curl） |

---

## License

MIT License. See [LICENSE](LICENSE) for details.
