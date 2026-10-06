// ============================================================================
// token_bucket.hpp - Token Bucket 限流器
// ============================================================================
// 基于 Token Bucket 算法的限流器, 用于保护 Gateway 免受过载。
// 支持全局和 per-IP 两级限流。
//
// 算法:
//   每个 token 代表一个请求许可。token 以固定速率 (rate) 生成,
//   最多累积到 capacity。请求到达时消耗一个 token,
//   如果 token 不足则拒绝请求 (429 Too Many Requests)。
//
// 线程安全: try_consume() 使用 std::atomic 实现无锁操作。
//
// 使用方式:
//   TokenBucket global_bucket(1000, 2000);  // 1000 req/s, burst 2000
//   TokenBucket ip_bucket(50, 100);          // per-IP: 50 req/s, burst 100
//
//   if (global_bucket.try_consume(1) && ip_bucket.try_consume(1)) {
//       // 处理请求
//   } else {
//       // 返回 429 Too Many Requests
//   }
// ============================================================================

#pragma once

#include <atomic>
#include <chrono>
#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <cstddef>

namespace cyrus {

// ============================================================================
// TokenBucket - 令牌桶限流器
// ============================================================================
class TokenBucket {
public:
    // 构造函数
    // rate: 令牌生成速率 (tokens/second)
    // capacity: 桶容量 (最大可累积 token 数, 即突发容量)
    explicit TokenBucket(double rate, double capacity)
        : rate_(rate)
        , capacity_(capacity)
        , tokens_(capacity)  // 初始满桶
        , last_refill_(std::chrono::steady_clock::now())
    {}

    // 尝试消费 tokens 个令牌
    // 返回 true 表示消费成功 (token 够)
    // 返回 false 表示被限流 (token 不够)
    bool try_consume(double tokens = 1.0) {
        refill();
        double expected = tokens_.load(std::memory_order_acquire);
        while (expected >= tokens) {
            if (tokens_.compare_exchange_weak(expected, expected - tokens,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;  // token 不足
    }

    // 动态调整速率 (用于自适应限流)
    void set_rate(double new_rate) {
        rate_.store(new_rate, std::memory_order_release);
    }

private:
    // 补充 token (基于经过的时间)
    void refill() {
        auto now = std::chrono::steady_clock::now();
        auto last = last_refill_.load(std::memory_order_acquire);

        // 计算经过的时间
        double elapsed = std::chrono::duration<double>(now.time_since_epoch()).count()
                       - std::chrono::duration<double>(last.time_since_epoch()).count();

        if (elapsed <= 0) return;

        // 尝试原子更新 last_refill_ (只有一个线程执行 refill)
        auto expected = last;
        if (!last_refill_.compare_exchange_strong(expected, now,
                                                   std::memory_order_release,
                                                   std::memory_order_relaxed)) {
            return;  // 其他线程已经 refill 了
        }

        // 计算新 token 数
        double new_tokens = tokens_.load(std::memory_order_acquire) + elapsed * rate_.load();
        if (new_tokens > capacity_) {
            new_tokens = capacity_;
        }

        tokens_.store(new_tokens, std::memory_order_release);
    }

    std::atomic<double> rate_;
    double capacity_;
    std::atomic<double> tokens_;
    std::atomic<std::chrono::steady_clock::time_point> last_refill_;
};

// ============================================================================
// RateLimiter - 两级限流器 (全局 + per-IP)
// ============================================================================
class RateLimiter {
public:
    RateLimiter(double global_rate, double global_capacity,
                double per_ip_rate, double per_ip_capacity)
        : global_(global_rate, global_capacity)
        , per_ip_rate_(per_ip_rate)
        , per_ip_capacity_(per_ip_capacity)
    {}

    // 检查请求是否被限流
    // client_ip: 客户端 IP 地址 (如 "192.168.1.1"), 空字符串仅检查全局
    // 返回 true 表示通过 (未被限流)
    // 返回 false 表示被限流 (应返回 429)
    bool check(const std::string& client_ip) {
        // 第一级: 全局限流
        if (!global_.try_consume(1)) {
            return false;
        }

        // 第二级: per-IP 限流
        if (!client_ip.empty()) {
            auto* bucket = get_or_create_ip_bucket(client_ip);
            if (!bucket->try_consume(1)) {
                // 注意: 全局 token 已被消费, 但不影响 (全局 token 应该够)
                return false;
            }
        }

        return true;
    }

private:
    TokenBucket* get_or_create_ip_bucket(const std::string& ip) {
        {
            std::lock_guard lock(ip_mutex_);
            auto it = ip_buckets_.find(ip);
            if (it != ip_buckets_.end()) {
                return it->second.get();
            }
        }

        std::lock_guard lock(ip_mutex_);
        auto it = ip_buckets_.find(ip);
        if (it != ip_buckets_.end()) {
            return it->second.get();
        }

        auto bucket = std::make_unique<TokenBucket>(per_ip_rate_, per_ip_capacity_);
        TokenBucket* ptr = bucket.get();
        ip_buckets_[ip] = std::move(bucket);
        return ptr;
    }

    TokenBucket global_;
    double per_ip_rate_;
    double per_ip_capacity_;

    mutable std::mutex ip_mutex_;
    std::unordered_map<std::string, std::unique_ptr<TokenBucket>> ip_buckets_;
};

} // namespace cyrus
