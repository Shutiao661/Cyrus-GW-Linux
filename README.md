# Cyrus-GW — 基于 C++20 协程与 io_uring 的流式 AI Gateway

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![Linux](https://img.shields.io/badge/Platform-Linux-blue.svg)](https://kernel.org)
[![io_uring](https://img.shields.io/badge/Engine-io__uring-green.svg)](https://kernel.dk/io_uring.pdf)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Cyrus-GW** 是一个用 C++20 编写的高性能异步 HTTP 网关，专为 LLM 流式场景设计。内置 **DeepSeek API 集成**，开箱即用。

---

## 架构设计

### Gateway / Agent 双服务隔离

解决 LLM 调用耗时长、流式连接持续时间长、错误链路难定位三大问题：**将接入层 I/O 与业务逻辑部署为独立进程，故障不传染，瓶颈可独立定位。**

```
                          ┌─────────────────────────────┐
     HTTP Clients ───────→│      Cyrus-GW Gateway       │
                          │   (io_uring + C++20 协程)    │
                          │                             │
                          │  · 连接接入                  │
                          │  · HTTP/1.1 解析 (状态机)     │
                          │  · Token Bucket 限流          │
                          │  · SSE 流式透传              │
                          └──────────┬──────────────────┘
                                     │ TCP (Binary Protocol)
                          ┌──────────▼──────────────────┐
                          │      Cyrus Agent            │
                          │                             │
                          │  · LLMProvider 策略模式      │
                          │  · DeepSeek API (真实 LLM)  │
                          │  · Mock LLM (测试用)         │
                          │  · 工具路由                  │
                          └─────────────────────────────┘
```

| 层 | 进程 | 职责 |
|----|------|------|
| **接入层** | Gateway | 连接接入、HTTP 解析、Token Bucket 限流、SSE 透传 |
| **业务层** | Agent | LLMProvider 策略、LLM 调用（DeepSeek/OpenAI/...）、工具路由 |

### LLMProvider 策略模式

Agent 通过 `LLMProvider` 抽象接口支持可插拔 LLM 后端：

| Provider | 说明 |
|----------|------|
| `DeepSeekProvider` | 通过 HTTPS + SSE 调用 DeepSeek API，解析流式响应 |
| `MockLLMProvider` | 内置模拟 LLM，返回预设 tokens，无需网络 |
| *自定义* | 实现 `LLMProvider::generate()` 即可接入任意 LLM |

```
ChatHandler.handle()
  │
  └─→ provider_->generate(prompt)
        │
        ├─ DeepSeekProvider → curl → api.deepseek.com/v1/chat/completions
        │                       │
        │                       └─ 解析 SSE "data:" 行 → 提取 delta.content
        │
        └─ MockLLMProvider  → 本地生成 tokens
```

### 并发模型：epoll vs io_uring + C++20 协程

| 指标 | epoll Reactor（基线） | io_uring + 协程 |
|------|----------------------|-----------------|
| c=500 完成请求数 | 1× | **2.15×** |
| 瓶颈定位 | — | Agent/LLM 调用侧 |

---

## 特性

- **DeepSeek API 集成**: 开箱即用，真实 LLM 流式调用
- **可插拔 LLMProvider**: 策略模式，对接任意 LLM 只需实现一个接口
- **io_uring 异步 I/O**: Linux 内核级零拷贝异步 I/O（无 liburing 时自动降级 POSIX fallback）
- **C++20 协程**: `co_await` 异步编程，Task/Awaitable 基础设施
- **HTTP/1.1 解析**: 逐字节有限状态机，支持 keep-alive、Content-Length、chunked TE
- **SSE 实时流式透传**: ChunkedDecoder 剥离传输编码 → 逐 token 推送，首字节延迟最低
- **三层超时保护**: 首包超时(15s) / 总超时(120s) / 帧间空闲(30s)，SSE header 后统一 `event: error`
- **OpenAI 兼容**: `POST /v1/chat/completions` 端点，标准 SSE 格式
- **二进制帧协议**: Gateway ↔ Agent 4B 长度前缀 + 16B MessageHeader 流式帧
- **缓冲池**: 预分配 RAII 管理，零 malloc 开销
- **Token Bucket 限流**: 全局 + 每 IP 双重限流
- **跨平台**: Linux io_uring / Windows IOCP 双引擎

---

## 快速开始

### 构建

```bash
git clone <repo-url> && cd Cyrus-GW

# 配置
cmake -B out/build/linux -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTS=ON

# 编译
cmake --build out/build/linux
```

### 运行

**终端 1 — Agent（LLM 业务层）**
```bash
./out/build/linux/cyrus_agent/cyrus_agent --port 9999
```

**终端 2 — Gateway（HTTP 接入层）**
```bash
./out/build/linux/cyrus_gateway/cyrus_gateway config/gateway.conf
```

### 测试

```bash
# 健康检查
curl http://localhost:8080/health
# → {"status":"ok","version":"2.0.0","platform":"Linux","engine":"io_uring"}

# 欢迎页面
curl http://localhost:8080/

# SSE 流式聊天 (DeepSeek)
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"你好"}]}'
# → 逐 token 实时流式输出...
```

### 停止

```bash
fuser -k 8080/tcp  # Gateway
fuser -k 9999/tcp  # Agent
```

---

## 构建目标

| 目标 | 类型 | 说明 |
|------|------|------|
| `cyrus_common` | 静态库 | 基础类型、日志、配置、缓冲池、协议、chunked 解码器 |
| `cyrus_gateway` | 可执行文件 | HTTP 网关 (io_uring/POSIX fallback + C++20 协程) |
| `cyrus_agent` | 可执行文件 | Agent 服务 (LLMProvider 策略架构) |
| `test_http_parser` | 测试 | HTTP 解析器 (9 cases) |
| `test_body_parser` | 测试 | Body 截断 + 粘包回归 (8 cases) |
| `test_chunked_decoder` | 测试 | Chunked 解码器 (8 cases) |
| `test_protocol` | 测试 | 协议编解码 (6 cases) |
| `test_sse` | 测试 | SSE 编解码 (6 cases) |
| `test_sse_error` | 测试 | SSE 错误处理 (10 cases) |
| `test_buffer` | 测试 | 缓冲池 (5 cases) |
| `test_token_bucket` | 测试 | Token Bucket 限流 (7 cases) |
| `cyrus_benchmark` | 基准测试 | wrk 同机压测 + epoll/io_uring 对照 |

### 运行测试

```bash
# 全部测试
ctest --test-dir out/build/linux --output-on-failure

# 单独测试
./out/build/linux/tests/test_http_parser
./out/build/linux/tests/test_body_parser
```

---

## 项目结构

```
Cyrus-GW/
├── CMakeLists.txt
├── CMakePresets.json
├── README.md
├── config/gateway.conf
├── cmake/CompilerWarnings.cmake
├── cyrus_common/                    # 静态库
│   ├── include/cyrus/
│   │   ├── types.hpp                # 核心类型、错误码
│   │   ├── buffer_pool.hpp          # 缓冲池 (RAII)
│   │   ├── chunked_decoder.hpp      # Chunked TE 解码器
│   │   ├── protocol.hpp             # 二进制帧协议
│   │   ├── sse_codec.hpp            # SSE 格式化
│   │   ├── token_bucket.hpp         # 限流器
│   │   ├── config.hpp               # 配置解析
│   │   ├── logger.hpp               # 日志系统
│   │   └── platform.hpp             # 平台抽象 (Win/Linux)
│   └── src/
│       ├── platform_win32.cpp       # Windows 信号处理
│       └── platform_linux.cpp       # Linux 信号处理
├── cyrus_gateway/                   # Gateway 可执行文件
│   ├── include/cyrus/gateway/
│   │   ├── server.hpp               # 服务器主控
│   │   ├── connection.hpp           # 连接状态机
│   │   ├── http_parser.hpp          # HTTP 解析器 (FSM)
│   │   ├── router.hpp               # 路由器 + Agent 连接池
│   │   ├── sse_handler.hpp          # SSE 中继 + 三层超时
│   │   ├── io_engine.hpp            # I/O 引擎抽象
│   │   ├── io_engine_uring.hpp      # io_uring 引擎
│   │   ├── io_engine_iocp.hpp       # IOCP 引擎 (Windows)
│   │   ├── coro_engine.hpp          # C++20 协程适配层
│   │   └── agent_client.hpp         # Agent TCP 客户端
│   └── src/
├── cyrus_agent/                     # Agent 可执行文件
│   ├── include/cyrus/agent/
│   │   ├── agent_server.hpp         # select 事件循环
│   │   ├── request_handler.hpp      # LLMProvider 抽象接口
│   │   ├── chat_handler.hpp         # ChatHandler + MockLLMProvider
│   │   ├── deepseek_provider.hpp    # DeepSeek API Provider
│   │   └── echo_handler.hpp         # Echo 调试处理器
│   └── src/
├── tests/                           # 单元测试 (8 个)
└── benchmark/                       # 基准测试
    ├── bench_epoll.cpp              # epoll Reactor 基线
    ├── bench_uring.cpp              # io_uring + 协程版
    ├── run_bench.sh                 # Linux 编排脚本
    └── run_bench.ps1                # Windows 编排脚本
```

---

## 配置

编辑 `config/gateway.conf`:

```ini
[server]
listen_address = 0.0.0.0
listen_port = 8080
worker_threads = 0              # 0 = auto (CPU core count)

[agent]
host = 127.0.0.1
port = 9999
pool_size = 4

[logging]
level = INFO                    # DEBUG | INFO | WARN | ERROR

[rate_limit]
global_rate = 1000
global_capacity = 2000
per_ip_rate = 50
per_ip_capacity = 100

[sse]
first_byte_timeout_ms = 15000   # 首包超时
total_timeout_ms = 120000       # 总超时
idle_timeout_ms = 30000         # 帧间空闲超时
```

---

## 技术栈

| 技术 | 用途 |
|------|------|
| C++20 | 核心语言 (`std::format`, coroutines, `std::source_location`) |
| io_uring | Linux 异步 I/O 引擎 (无 liburing 时 POSIX fallback) |
| C++20 Coroutines | `co_await` 异步编程模型 (Task\<T\>, Awaitable) |
| CMake + Ninja | 跨平台构建系统 |
| SSE | 流式推送协议 (OpenAI 兼容) |
| TCP | Gateway ↔ Agent 二进制帧协议 |
| DeepSeek API | 内置 LLM Provider，HTTPS + SSE |

---

## License

MIT License. See [LICENSE](LICENSE) for details.
