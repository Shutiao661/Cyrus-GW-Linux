// ============================================================================
// test_body_parser.cpp - HTTP Body 截断与流水线回归测试
// ============================================================================
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "../cyrus_gateway/include/cyrus/gateway/http_parser.hpp"

using namespace cyrus;
using namespace cyrus::gateway;

static int tests_passed = 0, tests_failed = 0;

#define TEST(name) printf("  TEST: %s ... ", name);
#define PASS() do { printf("PASSED\n"); tests_passed++; } while(0)
#define FAIL(msg) do { printf("FAILED: %s\n", msg); tests_failed++; } while(0)
#define CHECK(cond, msg) do { if (!(cond)) { FAIL(msg); return; } } while(0)

// 辅助: 构建简单 HTTP POST 请求
std::string make_request(const std::string& method, const std::string& uri,
                          const std::string& body,
                          const std::string& extra_headers = "") {
    std::string req = method + " " + uri + " HTTP/1.1\r\n";
    req += "Host: localhost\r\n";
    if (!body.empty()) {
        req += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    }
    if (!extra_headers.empty()) {
        req += extra_headers + "\r\n";
    }
    req += "\r\n";
    req += body;
    return req;
}

// 1. Content-Length 精确 body 读取
void test_content_length_exact() {
    TEST("Content-Length exact body read");
    HttpParser parser;
    std::string body = "Hello, World!";
    std::string req = make_request("POST", "/api", body);

    size_t consumed = parser.parse(
        reinterpret_cast<const uint8_t*>(req.data()), req.size());
    CHECK(parser.is_complete(), "Should be complete");
    CHECK(consumed == req.size(), "Should consume all bytes");

    const auto& result = parser.result();
    CHECK(result.has_content_length, "Should have Content-Length");
    CHECK(result.content_length() == 13, "Content-Length should be 13");
    CHECK(result.body_length == 13, "Body length should be 13");

    std::string parsed_body(result.body.begin(), result.body.end());
    CHECK(parsed_body == "Hello, World!", "Body should match");
    PASS();
}

// 2. Content-Length == 0 (GET 请求)
void test_zero_content_length() {
    TEST("Content-Length: 0 immediate completion");
    HttpParser parser;
    std::string req = make_request("GET", "/", "");

    size_t consumed = parser.parse(
        reinterpret_cast<const uint8_t*>(req.data()), req.size());
    CHECK(parser.is_complete(), "Should be complete");
    CHECK(consumed == req.size(), "Should consume all bytes");

    const auto& result = parser.result();
    CHECK(result.body_length == 0, "Body should be empty");
    PASS();
}

// 3. Body 半包到达 (分片)
void test_body_fragmented() {
    TEST("Body received in fragments");
    HttpParser parser;
    std::string body = std::string(1000, 'X');
    std::string req = make_request("POST", "/upload", body);

    // 第一片: 仅发送 header 部分
    size_t header_end = req.find("\r\n\r\n") + 4;
    size_t consumed1 = parser.parse(
        reinterpret_cast<const uint8_t*>(req.data()), header_end + 50);
    CHECK(!parser.is_complete(), "Should not be complete yet");
    CHECK(parser.state() == ParseState::BODY, "Should still be in BODY state");

    // 第二片: 发送剩余 body
    size_t consumed2 = parser.parse(
        reinterpret_cast<const uint8_t*>(req.data() + consumed1),
        req.size() - consumed1);
    CHECK(parser.is_complete(), "Should be complete after all data");
    CHECK(parser.result().body_length == 1000, "Should have full body");
    PASS();
}

// 4. 大 Content-Length body (不截断)
void test_large_body() {
    TEST("Large body not truncated");
    HttpParser parser;
    std::string body = std::string(50000, 'Z');
    std::string req = make_request("POST", "/big", body);

    size_t consumed = parser.parse(
        reinterpret_cast<const uint8_t*>(req.data()), req.size());
    CHECK(parser.is_complete(), "Should be complete");
    CHECK(parser.result().body_length == 50000, "Body should be full size");
    CHECK(consumed == req.size(), "Should consume all bytes");
    PASS();
}

// 5. 流水线解析 (两个请求粘在一起)
void test_pipelined_requests() {
    TEST("Pipelined requests parsing");
    HttpParser parser;
    std::string req1 = make_request("GET", "/health", "");
    std::string req2 = make_request("GET", "/", "");
    std::string combined = req1 + req2;

    // 解析第一个请求
    size_t consumed1 = parser.parse(
        reinterpret_cast<const uint8_t*>(combined.data()), combined.size());
    CHECK(parser.is_complete(), "First request should be complete");
    CHECK(parser.result().uri == "/health", "URI should be /health");
    // consumed1 应该小于 combined.size() (有剩余数据)
    CHECK(consumed1 < combined.size(), "Should have remaining bytes for second request");

    // 解析第二个请求 (模拟 connection 保留 pending data 再喂入)
    parser.reset();
    size_t remaining = combined.size() - consumed1;
    size_t consumed2 = parser.parse(
        reinterpret_cast<const uint8_t*>(combined.data() + consumed1), remaining);
    CHECK(parser.is_complete(), "Second request should be complete");
    CHECK(parser.result().uri == "/", "URI should be /");
    PASS();
}

// 6. Chunked body 大文件
void test_chunked_large() {
    TEST("Chunked encoding large body");
    HttpParser parser;

    // 构建 chunked 请求: 3 个 chunks, 每个 1000 字节
    std::string req;
    req += "POST /upload HTTP/1.1\r\n";
    req += "Host: localhost\r\n";
    req += "Transfer-Encoding: chunked\r\n";
    req += "\r\n";

    std::string data_a(1000, 'A');
    std::string data_b(1000, 'B');
    std::string data_c(1000, 'C');

    auto chunk = [](const std::string& data) -> std::string {
        std::string c;
        char hex[16];
        snprintf(hex, sizeof(hex), "%zx", data.size());
        c += hex;
        c += "\r\n";
        c += data;
        c += "\r\n";
        return c;
    };

    req += chunk(data_a);
    req += chunk(data_b);
    req += chunk(data_c);
    req += "0\r\n\r\n";

    size_t consumed = parser.parse(
        reinterpret_cast<const uint8_t*>(req.data()), req.size());
    CHECK(parser.is_complete(), "Should be complete");
    CHECK(parser.result().is_chunked, "Should be chunked");
    CHECK(parser.result().body_length == 3000, "Should decode 3000 bytes");
    PASS();
}

// 7. parse() 返回值正确 (非双重计数)
void test_parse_return_value() {
    TEST("parse() return value accuracy");
    HttpParser parser;
    std::string req = make_request("POST", "/test", "DATA");

    size_t consumed = parser.parse(
        reinterpret_cast<const uint8_t*>(req.data()), req.size());
    CHECK(consumed == req.size(), "Should consume exactly all bytes");
    CHECK(consumed <= req.size(), "Should not over-count bytes");
    PASS();
}

// 8. 空 body POST (Content-Length: 0 但 POST 方法)
void test_post_zero_body() {
    TEST("POST with Content-Length: 0");
    HttpParser parser;
    std::string req;
    req += "POST /api HTTP/1.1\r\n";
    req += "Host: localhost\r\n";
    req += "Content-Length: 0\r\n";
    req += "\r\n";

    size_t consumed = parser.parse(
        reinterpret_cast<const uint8_t*>(req.data()), req.size());
    CHECK(parser.is_complete(), "Should complete immediately");
    CHECK(parser.result().body_length == 0, "Body should be empty");
    CHECK(parser.result().has_content_length, "Should have Content-Length");
    PASS();
}

int main() {
    printf("=== HTTP Body Parser Regression Tests ===\n\n");
    test_content_length_exact();
    test_zero_content_length();
    test_body_fragmented();
    test_large_body();
    test_pipelined_requests();
    test_chunked_large();
    test_parse_return_value();
    test_post_zero_body();
    printf("\n=== Results: %d passed, %d failed ===\n", tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
