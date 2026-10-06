# ============================================================================
# cyrus_agent - Cyrus Agent 可执行文件
# 后端服务, 接收 Gateway 转发的请求, 执行实际业务逻辑
# ============================================================================

add_executable(cyrus_agent
    src/main.cpp
    src/agent_server.cpp       # Agent TCP 服务器 (select-based 事件循环)
)
# 注: echo_handler / chat_handler 为纯头文件实现 (全部 inline)

# 头文件搜索路径
target_include_directories(cyrus_agent PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# 链接 cyrus_common
target_link_libraries(cyrus_agent PRIVATE cyrus_common)

# 编译器警告
set_compiler_warnings(cyrus_agent)
