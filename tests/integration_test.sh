#!/usr/bin/env bash
# ============================================================================
# integration_test.sh - Cyrus-GW 端到端集成测试
# ============================================================================
# 启动 Agent (Mock provider) + Gateway, 验证:
#   1. GET /health 返回 200 + {"status":"ok"}
#   2. POST /v1/chat/completions 返回完整 SSE 流, 以 data: [DONE] 结束
#
# 用法: integration_test.sh <agent_bin> <gateway_bin>
# 依赖: bash, curl
# ============================================================================
set -u

AGENT_BIN="${1:-}"
GATEWAY_BIN="${2:-}"

if [[ -z "$AGENT_BIN" || -z "$GATEWAY_BIN" ]]; then
    echo "usage: $0 <agent_bin> <gateway_bin>" >&2
    exit 2
fi
if [[ ! -x "$AGENT_BIN" || ! -x "$GATEWAY_BIN" ]]; then
    echo "binaries not executable: $AGENT_BIN / $GATEWAY_BIN" >&2
    exit 2
fi

# 独立端口, 避免与开发环境冲突 (可通过环境变量覆盖)
AGENT_PORT="${CYRUS_TEST_AGENT_PORT:-19999}"
GATEWAY_PORT="${CYRUS_TEST_GATEWAY_PORT:-18080}"

TMPDIR_TEST="$(mktemp -d)"
CONF="$TMPDIR_TEST/gateway.conf"
AGENT_LOG="$TMPDIR_TEST/agent.log"
GATEWAY_LOG="$TMPDIR_TEST/gateway.log"

AG_PID=""
GW_PID=""

cleanup() {
    [[ -n "$GW_PID" ]] && kill "$GW_PID" 2>/dev/null
    [[ -n "$AG_PID" ]] && kill "$AG_PID" 2>/dev/null
    [[ -n "$GW_PID" ]] && wait "$GW_PID" 2>/dev/null
    [[ -n "$AG_PID" ]] && wait "$AG_PID" 2>/dev/null
    rm -rf "$TMPDIR_TEST"
}
trap cleanup EXIT

# --- 写入网关配置 (指向测试 Agent 端口) ---
cat > "$CONF" <<EOF
[server]
listen_address = 127.0.0.1
listen_port = $GATEWAY_PORT
worker_threads = 2

[agent]
host = 127.0.0.1
port = $AGENT_PORT
pool_size = 4

[logging]
level = WARN

[rate_limit]
global_rate = 10000
global_capacity = 20000
per_ip_rate = 10000
per_ip_capacity = 20000

[sse]
first_byte_timeout_ms = 15000
total_timeout_ms = 120000
idle_timeout_ms = 30000
EOF

# --- 工具: 等待端口就绪 (进程提前退出则失败) ---
wait_port() {
    local port="$1" pid="$2" log="$3"
    local i
    for i in $(seq 1 50); do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "process $pid exited early" >&2
            tail -20 "$log" >&2 || true
            return 1
        fi
        if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            exec 3>&- 3<&-
            return 0
        fi
        sleep 0.1
    done
    echo "port $port not ready in time" >&2
    tail -20 "$log" >&2 || true
    return 1
}

# --- 启动 Agent (无 DEEPSEEK_API_KEY → 自动回退 Mock provider) ---
"$AGENT_BIN" --port "$AGENT_PORT" > "$AGENT_LOG" 2>&1 &
AG_PID=$!

if ! wait_port "$AGENT_PORT" "$AG_PID" "$AGENT_LOG"; then
    echo "Agent failed to become ready" >&2
    exit 1
fi
echo "Agent ready on :$AGENT_PORT"

# --- 启动 Gateway ---
"$GATEWAY_BIN" "$CONF" > "$GATEWAY_LOG" 2>&1 &
GW_PID=$!

if ! wait_port "$GATEWAY_PORT" "$GW_PID" "$GATEWAY_LOG"; then
    echo "Gateway failed to become ready" >&2
    exit 1
fi
echo "Gateway ready on :$GATEWAY_PORT"

FAILED=0

# --- 测试 1: /health ---
HEALTH="$(curl -s --max-time 5 "http://127.0.0.1:$GATEWAY_PORT/health" || true)"
if echo "$HEALTH" | grep -q '"status":"ok"'; then
    echo "PASS: GET /health → $HEALTH"
else
    echo "FAIL: GET /health returned: [$HEALTH]" >&2
    FAILED=1
fi

# --- 测试 2: SSE 流式聊天 (Mock 返回多段 token) ---
SSE_OUT="$(curl -s -N --max-time 15 \
    -X POST "http://127.0.0.1:$GATEWAY_PORT/v1/chat/completions" \
    -H 'Content-Type: application/json' \
    -d '{"model":"mock","messages":[{"role":"user","content":"Hello"}]}' || true)"

if echo "$SSE_OUT" | grep -q 'data: \[DONE\]'; then
    DATA_LINES="$(echo "$SSE_OUT" | grep -c '^data:' || true)"
    echo "PASS: SSE stream complete ($DATA_LINES data lines, ends with [DONE])"
else
    echo "FAIL: SSE stream missing [DONE]" >&2
    echo "--- raw SSE ---" >&2
    echo "$SSE_OUT" >&2
    FAILED=1
fi

# --- 测试 3: /v1/chat 别名路由 ---
SSE_OUT2="$(curl -s -N --max-time 15 \
    -X POST "http://127.0.0.1:$GATEWAY_PORT/v1/chat" \
    -H 'Content-Type: application/json' \
    -d '{"model":"mock","messages":[{"role":"user","content":"Hi"}]}' || true)"

if echo "$SSE_OUT2" | grep -q 'data: \[DONE\]'; then
    echo "PASS: POST /v1/chat alias route complete"
else
    echo "FAIL: POST /v1/chat missing [DONE]" >&2
    FAILED=1
fi

if [[ $FAILED -ne 0 ]]; then
    echo "INTEGRATION TEST FAILED" >&2
    echo "--- gateway log ---" >&2
    tail -40 "$GATEWAY_LOG" >&2 || true
    echo "--- agent log ---" >&2
    tail -40 "$AGENT_LOG" >&2 || true
    exit 1
fi

echo "INTEGRATION TEST PASSED"
exit 0
