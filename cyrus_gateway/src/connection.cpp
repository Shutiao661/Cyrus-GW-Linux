// ============================================================================
// connection.cpp - 连接状态机实现
// ============================================================================

#include "cyrus/gateway/connection.hpp"
#include "cyrus/gateway/server.hpp"
#include "cyrus/gateway/sse_handler.hpp"
#include "cyrus/logger.hpp"

#include <poll.h>  // send_raw_sync 非阻塞 EAGAIN 等待可写

namespace cyrus {
namespace gateway {

// ============================================================================
// 构造/析构
// ============================================================================
Connection::Connection(socket_t fd, IOEngine* engine, BufferPool* pool,
                       Router* router, RateLimiter* rate_limiter,
                       std::string client_ip,
                       size_t max_keepalive_requests, size_t max_header_size)
    : fd_(fd)
    , engine_(engine)
    , pool_(pool)
    , router_(router)
    , rate_limiter_(rate_limiter)
    , client_ip_(std::move(client_ip))
    , max_keepalive_requests_(max_keepalive_requests)
{
    parser_.set_max_header_size(max_header_size);
    LOG_DEBUG("Connection created: fd={}", static_cast<int>(fd));
    last_activity_ = std::chrono::steady_clock::now();
}

Connection::~Connection() {
    if (fd_ != INVALID_SOCKET_VAL) {
        cyrus_close_socket(fd_);
        fd_ = INVALID_SOCKET_VAL;
    }
    // recv_buffer_ 和 send_buffer_ 自动归还给池 (RAII)
}

// ============================================================================
// on_accept_complete() - Accept 完成
// ============================================================================
// 新连接被接受后, 注册到引擎并投递第一个 recv
void Connection::on_accept_complete() {
    state_ = ConnectionState::READING_REQUEST;

    // 注册此 socket 到引擎 (这样后续的 recv/send 完成事件会路由到工作线程)
    engine_->register_socket(fd_, this);

    // 启动异步接收
    start_reading();
}

// ============================================================================
// start_reading() - 开始异步接收
// ============================================================================
// 从缓冲池获取一个缓冲区, 投递异步 recv
void Connection::start_reading() {
    // Body 读取超时检测: 如果长时间停留在 BODY/CHUNK 状态, 主动关闭
    auto now = std::chrono::steady_clock::now();
    if (parser_.state() == ParseState::BODY ||
        parser_.state() == ParseState::CHUNK_SIZE ||
        parser_.state() == ParseState::CHUNK_DATA) {
        if (body_read_start_.time_since_epoch().count() == 0) {
            body_read_start_ = now;  // 首次进入 body 状态, 记录时间
        } else {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - body_read_start_).count();
            if (elapsed > BODY_READ_TIMEOUT_MS) {
                LOG_WARN("Body read timeout for fd {} ({}ms)", static_cast<int>(fd_), elapsed);
                send_error(HttpStatus::REQUEST_TIMEOUT, "Request Body Read Timeout");
                return;
            }
        }
    } else {
        body_read_start_ = {};  // 非 body 状态, 重置计时器
    }

    // 获取接收缓冲区
    recv_buffer_ = pool_->acquire();
    if (!recv_buffer_) {
        LOG_ERROR("Failed to acquire recv buffer for fd {}", static_cast<int>(fd_));
        close();
        return;
    }

    // 投递异步 recv
    IOContext* ctx = engine_->acquire_context();
    ctx->buffer = recv_buffer_.data();
    ctx->buffer_len = recv_buffer_.capacity();
    ctx->user_data = this;

    int result = engine_->post_recv(fd_, ctx);
    if (result != 0) {
        LOG_ERROR("Failed to post recv for fd {}: err={}",
                  static_cast<int>(fd_), cyrus_socket_error());
        // 归还上下文
        engine_->release_context(ctx);
        close();
        return;
    }

    state_ = ConnectionState::READING_REQUEST;
}

// ============================================================================
// on_recv_complete() - 接收数据完成
// ============================================================================
// 将收到的数据喂入 HTTP 解析器, 检查是否完成解析
void Connection::on_recv_complete(IOContext* base_ctx) {
    IOContext* ctx = base_ctx;
    last_activity_ = std::chrono::steady_clock::now();

    if (ctx->bytes_transferred == 0 || ctx->error != 0) {
        // 连接关闭 (客户端断开) 或错误
        // 关键修复: 检测 body 截断 — 若解析器正在 BODY/CHUNK 状态且
        // 已读取部分 body 但未达到 Content-Length, 记录截断事件
        auto ps = parser_.state();
        if (ps == ParseState::BODY || ps == ParseState::CHUNK_SIZE ||
            ps == ParseState::CHUNK_DATA || ps == ParseState::CHUNK_TRAILER) {
            const auto& req = parser_.result();
            LOG_WARN("Body truncated on connection close: fd={}, "
                     "content_length={}, body_read={}, uri={}",
                     static_cast<int>(fd_),
                     req.has_content_length ? req.content_length() : 0,
                     req.body.size(),
                     req.uri);
        }
        LOG_DEBUG("Connection closed by client: fd={}, bytes={}, err={}",
                  static_cast<int>(fd_), ctx->bytes_transferred, ctx->error);
        close();
        engine_->release_context(ctx);
        return;
    }

    // 设置接收到的数据长度
    recv_buffer_.set_length(ctx->bytes_transferred);

    // --- 喂入 HTTP 解析器 ---
    size_t consumed = parser_.parse(recv_buffer_.data(), recv_buffer_.length());

    if (parser_.is_error()) {
        // HTTP 格式错误 → 返回 400
        LOG_WARN("HTTP parse error on fd {}: consumed {} of {} bytes",
                 static_cast<int>(fd_), consumed, recv_buffer_.length());
        send_error(HttpStatus::BAD_REQUEST, "Bad Request");
        engine_->release_context(ctx);
        recv_buffer_ = BufferHandle();  // 释放接收缓冲区
        return;
    }

    if (parser_.is_complete()) {
        // --- 请求解析完成 ---
        current_request_ = parser_.result();
        request_count_++;
        keep_alive_ = current_request_.keep_alive;

        // 关键修复: 保留未消费的流水线数据 (可能是下一个请求的开头)
        // 修复前此处丢弃了多余数据导致请求错位
        if (consumed < recv_buffer_.length()) {
            size_t remaining = recv_buffer_.length() - consumed;
            LOG_DEBUG("Pipelining: {} extra bytes preserved for next request on fd {}",
                      remaining, static_cast<int>(fd_));
            pending_data_.assign(recv_buffer_.data() + consumed,
                                 recv_buffer_.data() + recv_buffer_.length());
        }

        // 释放 IOContext 和接收缓冲区 (处理请求时会重新分配)
        engine_->release_context(ctx);
        recv_buffer_ = BufferHandle();

        // 重置 body 计时器
        body_read_start_ = {};

        // 处理请求
        handle_request();
    } else {
        // --- 需要更多数据 ---
        // 关键修复: 即使 parser 未完成, 也保留未消费的字节
        // (正常路径下 BODY 状态 consumed == len, 此检查为安全防护)
        if (consumed < recv_buffer_.length()) {
            size_t remaining = recv_buffer_.length() - consumed;
            LOG_DEBUG("Partial consume: {} bytes unconsumed in non-complete state on fd {}",
                      remaining, static_cast<int>(fd_));
            // 将未消费字节复制到 pending_data_ 头部 (保留旧的 pending_data_)
            std::vector<uint8_t> new_pending;
            new_pending.reserve(pending_data_.size() + remaining);
            new_pending.assign(recv_buffer_.data() + consumed,
                              recv_buffer_.data() + recv_buffer_.length());
            if (!pending_data_.empty()) {
                new_pending.insert(new_pending.end(),
                                  pending_data_.begin(), pending_data_.end());
            }
            pending_data_ = std::move(new_pending);
        }

        // 归还 ctx, 投递下一个 recv
        engine_->release_context(ctx);
        start_reading();
    }
}

// ============================================================================
// on_send_complete() - 发送完成
// ============================================================================
void Connection::on_send_complete(IOContext* ctx) {
    // 归还发送上下文
    engine_->release_context(ctx);

    // 释放发送缓冲区 (RAII 归还给池)
    send_buffer_ = BufferHandle();

    if (keep_alive_ && request_count_ < max_keepalive_requests_) {
        // --- keep-alive: 等待下一个请求 ---
        transition_to_idle();
    } else {
        // --- 关闭连接 ---
        if (request_count_ >= max_keepalive_requests_) {
            LOG_DEBUG("Max keep-alive requests reached for fd {}", static_cast<int>(fd_));
        }
        close();
    }
}

// ============================================================================
// on_timeout() - 空闲超时
// ============================================================================
void Connection::on_timeout() {
    LOG_DEBUG("Keep-alive timeout for fd {}", static_cast<int>(fd_));
    close();
}

// ============================================================================
// handle_request() - 处理完整的 HTTP 请求
// ============================================================================
// Gateway 接入层职责: 限流检查 → 路由分发 → 响应
// 业务逻辑委托给 Router, Router 进一步委托给 Agent (如需)
void Connection::handle_request() {
    const auto& req = current_request_;

    LOG_INFO("Request: {} {} from fd={}",
             http_method_to_sv(req.method), req.uri, static_cast<int>(fd_));

    // --- Token Bucket 限流检查 (全局 + per-IP) ---
    if (rate_limiter_) {
        if (!rate_limiter_->check(client_ip_)) {
            send_error(HttpStatus::SERVICE_UNAVAILABLE, "Rate limit exceeded");
            LOG_WARN("Rate limit exceeded for fd={} (ip={})",
                     static_cast<int>(fd_), client_ip_);
            return;
        }
    }

    // --- 路由分发: 委托给 Router ---
    if (router_) {
        bool routed = router_->route(this, req);
        if (!routed) {
            // Router 已内部调用 handle_404
        }
    } else {
        // Fallback: Router 未初始化时的最小路由
        if (req.method == HttpMethod::GET && req.uri == "/") {
            const char* welcome_html = R"(<!DOCTYPE html>
<html>
<head><title>Cyrus-GW</title><meta charset="utf-8"></head>
<body>
  <h1>Cyrus-GW Gateway v2.0</h1>
  <p>High-performance async HTTP gateway running on Linux io_uring</p>
  <ul>
    <li><a href="/health">GET /health</a> — Health check</li>
    <li>POST /v1/chat/completions — Chat completion (SSE streaming)</li>
  </ul>
  <p><small>Powered by C++20 + io_uring</small></p>
</body>
</html>)";
            send_response(HttpStatus::OK, "text/html; charset=utf-8", welcome_html);
        } else if (req.method == HttpMethod::GET && req.uri == "/health") {
            const char* health_json = R"({"status":"ok","version":"2.0.0","platform":"Linux"})";
            send_response(HttpStatus::OK, "application/json", health_json);
        } else {
            send_error(HttpStatus::NOT_FOUND, "Not Found");
        }
    }
}

// ============================================================================
// send_response() - 发送 HTTP 响应
// ============================================================================
void Connection::send_response(HttpStatus status,
                                const std::string& content_type,
                                std::string_view body) {
    std::string response = build_http_response(status, content_type, body, keep_alive_);

    // 从缓冲池获取发送缓冲区
    send_buffer_ = pool_->acquire();
    if (!send_buffer_ || send_buffer_.capacity() < response.size()) {
        LOG_ERROR("Failed to allocate send buffer or buffer too small for fd {}",
                  static_cast<int>(fd_));
        close();
        return;
    }

    // 复制响应数据到发送缓冲区
    std::memcpy(send_buffer_.data(), response.data(), response.size());
    send_buffer_.set_length(response.size());

    // 投递异步 send
    IOContext* ctx = engine_->acquire_context();
    ctx->buffer = send_buffer_.data();
    ctx->bytes_transferred = send_buffer_.length();  // 待发送长度
    ctx->user_data = this;

    int result = engine_->post_send(fd_, ctx);
    if (result != 0) {
        LOG_ERROR("Failed to post send for fd {}: err={}",
                  static_cast<int>(fd_), cyrus_socket_error());
        engine_->release_context(ctx);
        close();
        return;
    }

    state_ = ConnectionState::SENDING_RESPONSE;
}

// ============================================================================
// send_error() - 发送错误响应
// ============================================================================
void Connection::send_error(HttpStatus status, const char* message) {
    // 错误响应不 keep-alive
    keep_alive_ = false;

    // 构建简单的文本错误响应
    std::string body = std::format("{} {}\n",
                                    static_cast<uint16_t>(status),
                                    http_status_text(status));
    if (message) {
        body += message;
        body += "\n";
    }

    send_response(status, "text/plain", body);
}

// ============================================================================
// build_http_response() - 构建完整的 HTTP 响应字符串
// ============================================================================
std::string Connection::build_http_response(HttpStatus status,
                                             const std::string& content_type,
                                             std::string_view body,
                                             bool keep_alive) {
    // 状态行
    std::string resp;
    resp.reserve(256 + body.size());
    resp += "HTTP/1.1 ";
    resp += std::to_string(static_cast<uint16_t>(status));
    resp += " ";
    resp += http_status_text(status);
    resp += "\r\n";

    // 响应头
    resp += "Server: Cyrus-GW/2.0\r\n";
    resp += "Content-Type: ";
    resp += content_type;
    resp += "\r\n";
    resp += "Content-Length: ";
    resp += std::to_string(body.size());
    resp += "\r\n";
    resp += "Connection: ";
    resp += keep_alive ? "keep-alive" : "close";
    resp += "\r\n";
    resp += "\r\n";  // 头部结束

    // 响应体
    resp += body;

    return resp;
}

// ============================================================================
// transition_to_idle() - keep-alive 空闲状态
// ============================================================================
void Connection::transition_to_idle() {
    state_ = ConnectionState::IDLE;
    parser_.reset();

    // 关键修复: 优先处理流水线中保留的数据, 再投递新的 recv
    // 如果前一个请求的 recv 中包含了下一个请求的开头,
    // 这些数据已保存在 pending_data_ 中, 必须先喂入解析器
    if (!pending_data_.empty()) {
        LOG_DEBUG("Feeding {} bytes of pending pipeline data to parser on fd {}",
                  pending_data_.size(), static_cast<int>(fd_));
        size_t consumed = parser_.parse(pending_data_.data(), pending_data_.size());

        if (parser_.is_complete()) {
            // 流水线数据中已包含完整请求, 处理它
            // 关键修复: 检查是否还有未消费字节 (可能是第三个请求)
            // 先前直接 clear() 会丢弃 pending_data_ 中 req2+req3 的 req3 部分
            current_request_ = parser_.result();
            request_count_++;
            keep_alive_ = current_request_.keep_alive;
            if (consumed < pending_data_.size()) {
                // 保留未消费部分 (后续流水线请求)
                LOG_DEBUG("Pipelining: {} bytes preserved after complete from pending (fd={})",
                          pending_data_.size() - consumed, static_cast<int>(fd_));
                pending_data_.erase(pending_data_.begin(),
                                    pending_data_.begin() + consumed);
            } else {
                pending_data_.clear();
            }
            handle_request();
            return;
        } else if (parser_.is_error()) {
            send_error(HttpStatus::BAD_REQUEST, "Bad Request (pipelined)");
            pending_data_.clear();
            return;
        }
        // 流水线数据不完整, 继续等待更多数据
        // consumed 字节已被解析器消费, 保留未消费部分
        if (consumed < pending_data_.size()) {
            pending_data_.erase(pending_data_.begin(),
                                pending_data_.begin() + consumed);
        } else {
            pending_data_.clear();
        }
    }

    // 投递新的 recv
    start_reading();
}

// ============================================================================
// send_raw_sync() - 同步发送原始字节
// ============================================================================
// 用于 SSE 流式透传: 中继线程同步阻塞 Agent recv, 每收到一个 token 即
// 通过此方法立即推送给客户端, 实现真正的流式响应。
// 循环调用 ::send() 确保全部发送; 处理 EINTR (重试) 与 EAGAIN (等待可写)。
// 失败返回 false。
bool Connection::send_raw_sync(const void* data, size_t len) {
    if (fd_ == INVALID_SOCKET_VAL) return false;

    const char* ptr = static_cast<const char*>(data);
    size_t remaining = len;
    while (remaining > 0) {
        int sent = ::send(fd_, ptr, static_cast<int>(remaining), MSG_NOSIGNAL);
        if (sent > 0) {
            ptr += sent;
            remaining -= static_cast<size_t>(sent);
            continue;
        }

        int err = errno;
        if (err == CYRUS_EINTR) {
            continue;  // 被信号中断, 立即重试
        }
        if (err == CYRUS_EWOULDBLOCK) {  // == EAGAIN (Linux)
            // 非阻塞 socket 发送缓冲区已满 → 等待可写 (最长 1s)
            struct pollfd pfd{};
            pfd.fd = fd_;
            pfd.events = POLLOUT;
            int pr = ::poll(&pfd, 1, 1000);
            if (pr > 0) continue;                    // 可写, 重试
            if (pr < 0 && errno == CYRUS_EINTR) continue;  // 被信号打断, 重试
            LOG_ERROR("send_raw_sync: poll failed fd={}, pr={}, err={}",
                      static_cast<int>(fd_), pr, err);
            return false;
        }
        LOG_ERROR("send_raw_sync failed: fd={}, sent={}, err={}",
                  static_cast<int>(fd_), sent, err);
        return false;
    }
    return true;
}

// ============================================================================
// close() - 关闭连接
// ============================================================================
void Connection::close() {
    if (state_ == ConnectionState::CLOSED || state_ == ConnectionState::CLOSING) {
        return;  // 已经在关闭中
    }

    state_ = ConnectionState::CLOSING;
    LOG_DEBUG("Closing connection: fd={}, requests_handled={}",
              static_cast<int>(fd_), request_count_);

    // 关闭 socket
    if (fd_ != INVALID_SOCKET_VAL) {
        // 必须是 SHUT_RDWR (而非仅 SHUT_WR):
        // POSIX fallback 下 worker 线程同步阻塞在 ::recv 上, 而 shutdown(SHUT_WR)
        // 只影响写方向, 唤不醒阻塞的 ::recv; 从别的线程 close(fd) 同样唤不醒
        // (内核里已阻塞的系统调用仍持有该文件引用)。结果是 Server::stop()
        // 关闭连接后 join(worker) 会永久挂起。
        // SHUT_RDWR 会让阻塞中的 ::recv 立即返回, worker 得以退出。
        shutdown(fd_, SHUT_RDWR);
        cyrus_close_socket(fd_);
        fd_ = INVALID_SOCKET_VAL;
    }

    state_ = ConnectionState::CLOSED;
}

// ============================================================================
// idle_ms() - 空闲时长 (毫秒)
// ============================================================================
int64_t Connection::idle_ms() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - last_activity_).count();
}

} // namespace gateway
} // namespace cyrus
