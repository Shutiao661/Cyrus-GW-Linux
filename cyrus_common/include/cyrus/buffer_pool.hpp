// ============================================================================
// buffer_pool.hpp - 固定大小缓冲池
// ============================================================================
// 对象池 (Object Pool) 模式实现, 用于高性能网络 I/O:
//   - 预分配 N 个固定大小的缓冲区, 避免频繁 malloc/free
//   - RAII 自动归还: BufferHandle 析构时自动将缓冲区归还池
//   - 线程安全: 使用 mutex 保护空闲链表
//   - 高负载时池耗尽 → 返回 nullptr (不会阻塞)
//
// 为什么使用缓冲池?
//   IOCP/io_uring 的异步 I/O 需要预分配缓冲区。
//   每个 async recv 需要一个缓冲区保持有效直到完成事件到达。
//   频繁 new/delete 会: (1) 内存碎片 (2) 系统调用开销 (3) 延迟不可预测
//   缓冲池一次分配足够的内存, 之后都是 O(1) 的指针操作。
//
// 使用方式:
//   BufferPool pool(1024, 4096);  // 1024 个 4KB 缓冲区, 共 4MB
//   auto buf = pool.acquire();    // 获取一个缓冲区
//   if (buf) {
//       memcpy(buf->data(), data, len);
//       buf->set_length(len);
//       socket.send(buf->data(), buf->length());
//   }
//   // buf 离开作用域 → 自动归还给池
// ============================================================================

#pragma once

#include "types.hpp"
#include "logger.hpp"

#include <vector>
#include <memory>
#include <mutex>
#include <algorithm>

namespace cyrus {

// ============================================================================
// BufferHandle - 池分配的缓冲区的 RAII 句柄
// ============================================================================
// 持有缓冲区的所有权, 析构时自动归还给池。
// 不支持拷贝 (唯一所有权), 支持移动。
// ============================================================================
class BufferPool;  // 前向声明

class BufferHandle {
public:
    friend class BufferPool;

    // 默认构造: 空句柄
    BufferHandle() = default;

    // 析构: 自动归还缓冲区给池
    ~BufferHandle() {
        release();
    }

    // 禁止拷贝
    BufferHandle(const BufferHandle&) = delete;
    BufferHandle& operator=(const BufferHandle&) = delete;

    // 支持移动
    BufferHandle(BufferHandle&& other) noexcept
        : pool_(other.pool_)
        , buffer_index_(other.buffer_index_)
        , data_(other.data_)
        , capacity_(other.capacity_)
        , length_(other.length_)
    {
        other.pool_ = nullptr;
        other.buffer_index_ = -1;
        other.data_ = nullptr;
        other.capacity_ = 0;
        other.length_ = 0;
    }

    BufferHandle& operator=(BufferHandle&& other) noexcept {
        if (this != &other) {
            release();           // 先归还当前持有的缓冲区
            pool_ = other.pool_;
            buffer_index_ = other.buffer_index_;
            data_ = other.data_;
            capacity_ = other.capacity_;
            length_ = other.length_;
            other.pool_ = nullptr;
            other.buffer_index_ = -1;
            other.data_ = nullptr;
            other.capacity_ = 0;
            other.length_ = 0;
        }
        return *this;
    }

    // --- 数据访问 ---
    uint8_t* data() noexcept { return data_; }
    const uint8_t* data() const noexcept { return data_; }

    // 容量 (缓冲区总大小)
    size_t capacity() const noexcept { return capacity_; }

    // 有效数据长度 (由用户设置, 例如 recv 后设置实际接收的字节数)
    size_t length() const noexcept { return length_; }
    void set_length(size_t len) noexcept {
        if (len <= capacity_) length_ = len;
    }

    // 检查句柄是否有效 (是否持有缓冲区)
    bool valid() const noexcept { return data_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }

private:
    // 构造函数 (仅 BufferPool 可调用)
    BufferHandle(BufferPool* pool, int index, uint8_t* data, size_t capacity)
        : pool_(pool)
        , buffer_index_(index)
        , data_(data)
        , capacity_(capacity)
        , length_(0)
    {}

    // 归还缓冲区给池
    void release();

    BufferPool* pool_ = nullptr;
    int buffer_index_ = -1;       // 缓冲区在池中的索引 (用于归还)
    uint8_t* data_ = nullptr;     // 缓冲区指针
    size_t capacity_ = 0;         // 缓冲区总容量
    size_t length_ = 0;           // 有效数据长度
};

// ============================================================================
// BufferPool - 固定大小缓冲池
// ============================================================================
class BufferPool {
public:
    // --- 配置常量 ---
    static constexpr size_t DEFAULT_BUFFER_SIZE  = 4096;   // 默认每缓冲区 4KB
    static constexpr size_t DEFAULT_BUFFER_COUNT = 1024;   // 默认 1024 个缓冲区 (共 4MB)

    // --- 构造函数 ---
    // count: 缓冲区数量
    // buffer_size: 每个缓冲区的大小 (字节)
    explicit BufferPool(size_t count = DEFAULT_BUFFER_COUNT,
                        size_t buffer_size = DEFAULT_BUFFER_SIZE)
        : buffer_size_(buffer_size)
    {
        // 一次性分配所有缓冲区所需的内存 (一个大块, 减少 malloc 次数)
        memory_block_.resize(count * buffer_size);

        // 初始化空闲链表: 所有索引 0..count-1 都是可用的
        free_list_.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            free_list_.push_back(static_cast<int>(i));
        }

        LOG_INFO("BufferPool created: {} buffers x {} bytes = {} MB",
                 count, buffer_size, (count * buffer_size) / (1024 * 1024));
    }

    // 禁止拷贝
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    // --- 获取缓冲区 ---
    // 返回一个 RAII 句柄, 句柄析构时自动归还
    // 如果池耗尽, 返回空句柄 (valid() == false)
    BufferHandle acquire() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (free_list_.empty()) {
            // 池耗尽: 返回空句柄
            // 调用者应检查 valid() 并适当处理 (如返回 503 Service Unavailable)
            LOG_WARN("BufferPool exhausted! Increase buffer count.");
            return BufferHandle();
        }

        // 从空闲链表取一个索引
        int index = free_list_.back();
        free_list_.pop_back();

        // 计算该缓冲区在内存块中的偏移
        uint8_t* buf_ptr = memory_block_.data() + (static_cast<size_t>(index) * buffer_size_);

        return BufferHandle(this, index, buf_ptr, buffer_size_);
    }

    // --- 获取大小为 count 的 iovec 数组 (用于 io_uring register_buffers) ---
    // 仅在 Linux 上使用, Windows 上保留接口
    std::vector<std::pair<uint8_t*, size_t>> get_buffer_info() const {
        std::vector<std::pair<uint8_t*, size_t>> result;
        size_t count = memory_block_.size() / buffer_size_;
        for (size_t i = 0; i < count; ++i) {
            result.emplace_back(
                const_cast<uint8_t*>(memory_block_.data() + i * buffer_size_),
                buffer_size_);
        }
        return result;
    }

    // --- 统计信息 ---
    size_t total_count() const {
        return memory_block_.size() / buffer_size_;
    }

    size_t available_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return free_list_.size();
    }

    size_t buffer_size() const noexcept { return buffer_size_; }

private:
    friend class BufferHandle;

    // --- 归还缓冲区到池 (由 BufferHandle::release() 调用) ---
    void release_buffer(int index) {
        std::lock_guard<std::mutex> lock(mutex_);
        free_list_.push_back(index);
    }

    // 所有缓冲区的内存块 (连续分配, 更好的缓存局部性)
    std::vector<uint8_t> memory_block_;

    // 空闲缓冲区索引列表 (栈式, LIFO)
    std::vector<int> free_list_;

    // 每个缓冲区大小
    size_t buffer_size_;

    // 线程安全锁
    mutable std::mutex mutex_;
};

// ============================================================================
// BufferHandle::release() 实现 (需要在 BufferPool 完整定义之后)
// ============================================================================
inline void BufferHandle::release() {
    if (pool_ && buffer_index_ >= 0) {
        pool_->release_buffer(buffer_index_);
        pool_ = nullptr;
        buffer_index_ = -1;
        data_ = nullptr;
        capacity_ = 0;
        length_ = 0;
    }
}

} // namespace cyrus
