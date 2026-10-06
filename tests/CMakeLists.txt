# ============================================================================
# tests/CMakeLists.txt - 单元测试
# 使用 CTest 集成, 在 VS Test Explorer 中可直接运行
# ============================================================================

# 辅助宏: 添加一个测试
# test_name: 测试名称 (也是可执行文件名)
# test_source: 测试源文件
macro(add_cyrus_test test_name test_source)
    add_executable(${test_name} ${test_source})
    target_link_libraries(${test_name} PRIVATE cyrus_common)
    set_compiler_warnings(${test_name})
    add_test(NAME ${test_name} COMMAND ${test_name})
endmacro()

# --- 测试目标 ---
# test_http_parser 和 test_body_parser 需要编译 http_parser.cpp 实现
add_executable(test_http_parser test_http_parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../cyrus_gateway/src/http_parser.cpp)
target_link_libraries(test_http_parser PRIVATE cyrus_common)
target_include_directories(test_http_parser PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../cyrus_gateway/include)
set_compiler_warnings(test_http_parser)
add_test(NAME test_http_parser COMMAND test_http_parser)

add_cyrus_test(test_protocol      test_protocol.cpp)
# test_sse 需要链接 sse_handler.cpp (SSEHandler::feed 的实现)
add_executable(test_sse test_sse.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../cyrus_gateway/src/sse_handler.cpp)
target_link_libraries(test_sse PRIVATE cyrus_common)
target_include_directories(test_sse PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../cyrus_gateway/include)
set_compiler_warnings(test_sse)
add_test(NAME test_sse COMMAND test_sse)
add_cyrus_test(test_buffer        test_buffer.cpp)

# v2.0 新增测试
add_cyrus_test(test_token_bucket    test_token_bucket.cpp)
add_cyrus_test(test_chunked_decoder test_chunked_decoder.cpp)

# test_sse_error 需要 gateway 头文件 (SSERelayHandler 在 sse_handler.hpp 中)
add_executable(test_sse_error test_sse_error.cpp)
target_link_libraries(test_sse_error PRIVATE cyrus_common)
target_include_directories(test_sse_error PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../cyrus_gateway/include)
set_compiler_warnings(test_sse_error)
add_test(NAME test_sse_error COMMAND test_sse_error)

# test_body_parser 需要链接 http_parser.cpp 实现
add_executable(test_body_parser test_body_parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../cyrus_gateway/src/http_parser.cpp)
target_link_libraries(test_body_parser PRIVATE cyrus_common)
target_include_directories(test_body_parser PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../cyrus_gateway/include)
set_compiler_warnings(test_body_parser)
add_test(NAME test_body_parser COMMAND test_body_parser)

# --- 端到端集成测试 ---
# 启动 Agent(Mock) + Gateway 两个可执行文件, 用 curl 验证 /health 与 SSE 流。
# 依赖 bash + curl; 端口默认 19999/18080, 可用环境变量覆盖。
if(TARGET cyrus_gateway AND TARGET cyrus_agent)
    add_test(NAME integration_test
        COMMAND bash
            ${CMAKE_CURRENT_SOURCE_DIR}/integration_test.sh
            $<TARGET_FILE:cyrus_agent>
            $<TARGET_FILE:cyrus_gateway>)
    set_tests_properties(integration_test PROPERTIES
        TIMEOUT 90
        LABELS "integration")
endif()
