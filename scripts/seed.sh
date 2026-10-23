#!/bin/sh
# Seeds Redis with one demo customer, lists and keywords.
# Usage: REDIS_HOST=localhost API_KEY=demo-key ./scripts/seed.sh
set -eu

R="redis-cli -h ${REDIS_HOST:-localhost} -p ${REDIS_PORT:-6379} -n ${REDIS_DB:-0}"
API_KEY="${API_KEY:-demo-key}"
CUSTOMER="${CUSTOMER:-cust-demo}"
CALLBACK="${CALLBACK_URL:-http://mockrelay:9000/callback}"
KEY_HASH=$(printf '%s' "$API_KEY" | sha256sum | cut -d' ' -f1)

$R SET "xray:apikey:$KEY_HASH" "$CUSTOMER"
$R HSET "xray:customer:$CUSTOMER" \
  level 5 rate_limit 1000 callback_url "$CALLBACK" \
  keyword_action HOLD ai_block 0.9 ai_hold 0.7

$R SADD xray:bl evil.example phish.example "+10000000000"
$R SADD xray:wl example.org "+19999999999"
$R SADD xray:keywords "free money" "lottery winner" "verify your account" "send otp"

echo "seeded customer $CUSTOMER (api key: $API_KEY)"
