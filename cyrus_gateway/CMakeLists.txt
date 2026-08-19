# ============================================================================
# cyrus_gateway - HTTP 网关可执行文件
# 依赖 cyrus_common 静态库
# ============================================================================

add_executable(cyrus_gateway
    src/main.cpp
    src/io_engine_iocp.cpp        # IOCP 引擎 (Windows 核心实现)
    src/io_engine_uring.cpp       # io_uring 存根 (非 Linux 平台编译用)
    src/http_parser.cpp           # HTTP/1.1 解析器状态机
    src/connection.cpp            # 连接状态机
    src/server.cpp                # 服务器主控
    src/router.cpp                # URL 路由 + Agent 连接池
    src/sse_handler.cpp           # SSE 流处理器
    src/agent_client.cpp          # Agent TCP 客户端
)

# 头文件搜索路径
target_include_directories(cyrus_gateway PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# 链接 cyrus_common (也会自动链接 Winsock2 / pthread)
target_link_libraries(cyrus_gateway PRIVATE cyrus_common)

# Linux: 尝试链接 liburing (可选, 用于 io_uring 引擎)
if(NOT WIN32)
    find_library(LIBURING_LIB uring)
    if(LIBURING_LIB)
        target_link_libraries(cyrus_gateway PRIVATE ${LIBURING_LIB})
        message(STATUS "Found liburing: ${LIBURING_LIB}")
    else()
        message(STATUS "liburing not found — io_uring engine will use fallback")
    endif()
endif()

# 编译器警告
set_compiler_warnings(cyrus_gateway)
