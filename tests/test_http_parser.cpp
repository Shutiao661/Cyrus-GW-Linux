// ============================================================================
// test_http_parser.cpp - HTTP 解析器单元测试
// ============================================================================
// 测试用例:
//   1. 简单的 GET 请求
//   2. POST 请求 (Content-Length 精确字节)
//   3. Content-Length: 0
//   4. 分片到达 (模拟 TCP 拆包)
//   5. keep-alive 检测 (Connection: keep-alive)
//   6. Connection: close 检测
//   7. 错误: 方法为空
//   8. 解析器重置 (用于 keep-alive 复用)
//   9. 管线化检测
//  10. Chunked 传输编码
// ============================================================================

#include <cstdio>
#include <cassert>
#include <cstring>
#include <string>

// 直接 include 源文件 (或者链接 gateway 库)
// 测试中直接使用 header-only 部分
#include "../cyrus_gateway/include/cyrus/gateway/http_parser.hpp"

using namespace cyrus;
using namespace cyrus::gateway;

// 测试辅助宏
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    printf("  TEST: %s ... ", name);

#define PASS() \
    do { printf("PASSED\n"); tests_passed++; } while(0)

#define FAIL(msg) \
    do { printf("FAILED: %s\n", msg); tests_failed++; } while(0)

#define CHECK(cond, msg) \
    do { if (!(cond)) { FAIL(msg); return; } } while(0)

// ========================================================================
// 测试用例
// ========================================================================

// 1. 简单 GET 请求
void test_simple_get() {
    TEST("Simple GET request");

    HttpParser parser;
    const char* raw = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
    size_t consumed = parser.parse(reinterpret_cast<const uint8_t*>(raw), strlen(raw));

    CHECK(parser.is_complete(), "Should be complete");
    CHECK(!parser.is_error(), "Should not have error");
    CHECK(consumed == strlen(raw), "Should consume all bytes");

    const auto& req = parser.result();
    CHECK(req.method == HttpMethod::GET, "Method should be GET");
    CHECK(req.uri == "/", "URI should be /");
    CHECK(req.version == "HTTP/1.1", "Version should be HTTP/1.1");
    CHECK(req.keep_alive == true, "Should keep-alive by default for HTTP/1.1");

    // 检查 Host 头
    auto host = req.header("Host");
    CHECK(host == "localhost", "Host header should be localhost");

    PASS();
}

// 2. POST 请求 + Content-Length
void test_post_with_body() {
    TEST("POST with Content-Length");

    HttpParser parser;
    const char* raw =
        "POST /api/data HTTP/1.1\r\n"
        "Host: example.com\r\n"
        "Content-Length: 13\r\n"
        "\r\n"
        "Hello, World!";
    size_t consumed = parser.parse(reinterpret_cast<const uint8_t*>(raw), strlen(raw));

    CHECK(parser.is_complete(), "Should be complete");
    CHECK(!parser.is_error(), "Should not have error");

    const auto& req = parser.result();
    CHECK(req.method == HttpMethod::POST, "Method should be POST");
    CHECK(req.uri == "/api/data", "URI should be /api/data");
    CHECK(req.has_content_length == true, "Should have Content-Length");
    CHECK(req.content_length() == 13, "Content-Length should be 13");
    CHECK(req.body_length == 13, "Body length should be 13");

    std::string body(req.body.begin(), req.body.end());
    CHECK(body == "Hello, World!", "Body should match");

    PASS();
}

// 3. Content-Length: 0 (无请求体)
void test_content_length_zero() {
    TEST("Content-Length: 0");

    HttpParser parser;
    const char* raw =
        "GET /empty HTTP/1.1\r\n"
        "Content-Length: 0\r\n"
        "\r\n";
    size_t consumed = parser.parse(reinterpret_cast<const uint8_t*>(raw), strlen(raw));

    CHECK(parser.is_complete(), "Should be complete");
    CHECK(!parser.is_error(), "Should not have error");

    const auto& req = parser.result();
    CHECK(req.body_length == 0, "Body should be empty");

    PASS();
}

// 4. 分片到达 (模拟 TCP 拆包)
void test_partial_reads() {
    TEST("Partial reads (TCP fragmentation)");

    HttpParser parser;
    size_t consumed;

    // 第一片: 方法 + URI
    const char* part1 = "GET /partial HTTP/1.1\r\n";
    consumed = parser.parse(reinterpret_cast<const uint8_t*>(part1), strlen(part1));
    CHECK(!parser.is_complete() && !parser.is_error(), "Should not be complete after part 1");

    // 第二片: 头部 + 空行
    const char* part2 = "Host: test\r\n\r\n";
    consumed = parser.parse(reinterpret_cast<const uint8_t*>(part2), strlen(part2));
    CHECK(parser.is_complete(), "Should be complete after part 2");
    CHECK(!parser.is_error(), "Should not have error");

    const auto& req = parser.result();
    CHECK(req.uri == "/partial", "URI should be /partial");

    PASS();
}

// 5. Connection: keep-alive
void test_keep_alive() {
    TEST("Connection: keep-alive");

    HttpParser parser;
    const char* raw =
        "GET /keep HTTP/1.1\r\n"
        "Host: test\r\n"
        "Connection: keep-alive\r\n"
        "\r\n";
    parser.parse(reinterpret_cast<const uint8_t*>(raw), strlen(raw));

    CHECK(parser.is_complete(), "Should be complete");
    CHECK(parser.result().keep_alive == true, "Should keep-alive");

    PASS();
}

// 6. Connection: close
void test_connection_close() {
    TEST("Connection: close");

    HttpParser parser;
    const char* raw =
        "GET /close HTTP/1.0\r\n"  // HTTP/1.0 默认不 keep-alive
        "Host: test\r\n"
        "\r\n";
    parser.parse(reinterpret_cast<const uint8_t*>(raw), strlen(raw));

    CHECK(parser.is_complete(), "Should be complete");
    // HTTP/1.0 默认 Connection: close (除非显式声明 Keep-Alive)
    CHECK(parser.result().keep_alive == false, "HTTP/1.0 should not keep-alive by default");

    PASS();
}

// 7. 错误: 空方法
void test_error_empty_method() {
    TEST("Error: empty method");

    HttpParser parser;
    const char* raw = " / HTTP/1.1\r\n\r\n";  // 方法为空 (以空格开头)
    parser.parse(reinterpret_cast<const uint8_t*>(raw), strlen(raw));

    CHECK(parser.is_error(), "Should have error for empty method");

    PASS();
}

// 8. 解析器重置
void test_parser_reset() {
    TEST("Parser reset for keep-alive");

    HttpParser parser;

    // 第一个请求
    const char* req1 = "GET /first HTTP/1.1\r\nHost: a\r\n\r\n";
    parser.parse(reinterpret_cast<const uint8_t*>(req1), strlen(req1));
    CHECK(parser.is_complete(), "First request should complete");
    std::string uri1 = parser.result().uri;

    // 重置
    parser.reset();
    CHECK(parser.state() == ParseState::METHOD, "Should reset to METHOD state");

    // 第二个请求
    const char* req2 = "GET /second HTTP/1.1\r\nHost: b\r\n\r\n";
    parser.parse(reinterpret_cast<const uint8_t*>(req2), strlen(req2));
    CHECK(parser.is_complete(), "Second request should complete");
    CHECK(parser.result().uri == "/second", "URI should be /second");

    PASS();
}

// 9. Chunked 传输编码
void test_chunked_encoding() {
    TEST("Chunked Transfer-Encoding");

    HttpParser parser;
    const char* raw =
        "POST /chunked HTTP/1.1\r\n"
        "Host: test\r\n"
        "Transfer-Encoding: chunked\r\n"
        "\r\n"
        "7\r\n"           // chunk 大小: 7 字节
        "Mozilla\r\n"     // chunk 数据 (7 字节)
        "9\r\n"           // chunk 大小: 9 字节
        "Developer\r\n"   // chunk 数据 (9 字节)
        "0\r\n"           // 终止 chunk
        "\r\n";           // 尾部空行

    size_t consumed = parser.parse(reinterpret_cast<const uint8_t*>(raw), strlen(raw));

    CHECK(parser.is_complete(), "Should be complete");
    CHECK(!parser.is_error(), "Should not have error");

    const auto& req = parser.result();
    CHECK(req.is_chunked == true, "Should detect chunked encoding");
    CHECK(req.body_length == 16, "Body should be 16 bytes (7+9)");

    std::string body(req.body.begin(), req.body.end());
    CHECK(body == "MozillaDeveloper", "Body should be concatenated chunks");

    PASS();
}

// ========================================================================
// Main
// ========================================================================
int main() {
    printf("========================================\n");
    printf("  HTTP Parser Unit Tests\n");
    printf("========================================\n\n");

    test_simple_get();
    test_post_with_body();
    test_content_length_zero();
    test_partial_reads();
    test_keep_alive();
    test_connection_close();
    test_error_empty_method();
    test_parser_reset();
    test_chunked_encoding();

    printf("\n========================================\n");
    printf("  Results: %d passed, %d failed\n", tests_passed, tests_failed);
    printf("========================================\n");

    return tests_failed > 0 ? 1 : 0;
}
