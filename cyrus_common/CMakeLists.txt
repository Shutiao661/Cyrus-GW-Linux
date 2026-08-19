# ============================================================================
# cyrus_common - Cyrus-GW 公共库 (静态库)
# 提供所有模块共享的基础设施: 类型定义、平台抽象、日志、配置、缓冲池、协议
# ============================================================================

# 平台特定源文件
if(WIN32)
    set(PLATFORM_SRC src/platform_win32.cpp)
else()
    set(PLATFORM_SRC src/platform_linux.cpp)
endif()

# 定义静态库目标
add_library(cyrus_common STATIC
    src/logger.cpp          # 日志框架实现 (C++20 std::format)
    src/config.cpp          # 配置解析器 (INI 风格)
    src/buffer_pool.cpp     # 缓冲池 (RAII, 线程安全)
    src/protocol.cpp        # 二进制协议编解码 (4字节长度前缀 + 载荷)
    ${PLATFORM_SRC}         # 平台特定 (WSA/信号处理)
)

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
# Windows: 链接 Winsock2 库
# PUBLIC: 链接此库的目标 (gateway, agent) 也会自动链接 ws2_32 和 mswsock
# ---------------------------------------------------------------------------
if(WIN32)
    target_link_libraries(cyrus_common PUBLIC ${WINSOCK_LIBRARIES})
else()
    # Linux: 链接 pthread (用于 std::thread 等)
    find_package(Threads REQUIRED)
    target_link_libraries(cyrus_common PUBLIC Threads::Threads)
endif()
