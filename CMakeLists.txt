# ============================================================================
# Cyrus-GW 根 CMakeLists.txt
# 一个基于异步 I/O 的高性能 HTTP 网关，支持 SSE 流式响应
# 构建: cmake -B build && cmake --build build
# ============================================================================
cmake_minimum_required(VERSION 3.21)

# ---------------------------------------------------------------------------
# 项目定义
# - VERSION: 语义化版本号
# - LANGUAGES CXX: 纯 C++ 项目
# ---------------------------------------------------------------------------
project(CyrusGW
    VERSION 2.0.0
    DESCRIPTION "High-performance async HTTP gateway with SSE streaming"
    LANGUAGES CXX
)

# ============================================================================
# C++ 标准配置
# ============================================================================
# 强制使用 C++20 (需要 std::format, concepts 等特性)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)   # 编译器不支持 C++20 则报错
set(CMAKE_CXX_EXTENSIONS OFF)         # 禁用编译器扩展 (保证可移植性)

# 生成 compile_commands.json 供 clangd/IDE 使用
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ============================================================================
# 编译选项
# ============================================================================
# 可配置的构建选项
option(BUILD_TESTS      "Build unit tests"      ON)
option(BUILD_BENCHMARKS "Build benchmarks"       ON)
option(USE_ASAN         "Enable AddressSanitizer" OFF)

# AddressSanitizer 支持 (GCC/Clang)
if(USE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

# ============================================================================
# CMake 模块路径
# ============================================================================
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(CompilerWarnings)

# ============================================================================
# 子目录 (按依赖顺序)
# ============================================================================
add_subdirectory(cyrus_common)       # 1. 静态库: 基础类型/日志/配置/缓冲池/协议

# 网关和 Agent 可独立构建，也可一起构建
add_subdirectory(cyrus_gateway)      # 2. 可执行文件: HTTP 网关
add_subdirectory(cyrus_agent)        # 3. 可执行文件: 后端 Agent

# 可选组件
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(BUILD_BENCHMARKS)
    add_subdirectory(benchmark)
endif()
