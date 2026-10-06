# ============================================================================
# CompilerWarnings.cmake - 编译器警告配置
# 为每个目标设置合理的警告级别和编译选项
# ============================================================================

# ---------------------------------------------------------------------------
# set_compiler_warnings(target_name)
#   为目标设置高警告级别和其他编译选项
#   - GCC/Clang: -Wall -Wextra -Wpedantic 等
# ---------------------------------------------------------------------------
function(set_compiler_warnings target_name)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${target_name} PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Wshadow
            -Wnon-virtual-dtor
            -Wcast-align
            -Woverloaded-virtual
            -Wconversion
            -Wsign-conversion
            -Wnull-dereference
            -Wdouble-promotion
            -Wformat=2
            -Wimplicit-fallthrough
            -Wno-c++20-compat
        )
    endif()
endfunction()
