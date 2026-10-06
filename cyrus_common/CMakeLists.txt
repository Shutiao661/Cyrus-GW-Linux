# ============================================================================
# cyrus_common - Cyrus-GW 公共库 (静态库)
# 提供所有模块共享的基础设施: 类型定义、平台抽象、日志、配置、缓冲池、协议
# ============================================================================

# 定义静态库目标
add_library(cyrus_common STATIC
    src/protocol.cpp        # 二进制协议编解码 (4字节长度前缀 + 载荷)
    src/platform_linux.cpp  # Linux 平台特定实现 (信号处理)
)
# 注: logger / config / buffer_pool 为纯头文件实现 (全部 inline)

# ---------------------------------------------------------------------------
# 头文件搜索路径
# PUBLIC: 链接此库的目标也会继承此路径
# cyrus_common/include → 使用者用 #include <cyrus/types.hpp>
# ---------------------------------------------------------------------------
target_include_directories(cyrus_common PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# 编译器警告
set_compiler_warnings(cyrus_common)

# ---------------------------------------------------------------------------
# Linux: 链接 pthread (用于 std::thread 等)
# PUBLIC: 链接此库的目标 (gateway, agent) 也会自动链接 pthread
# ---------------------------------------------------------------------------
find_package(Threads REQUIRED)
target_link_libraries(cyrus_common PUBLIC Threads::Threads)
