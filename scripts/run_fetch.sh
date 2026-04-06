#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

# 先加载.env（作为默认兜底）
if [ -f "$ENV_FILE" ]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

# 优先从 1Password 刷新关键凭证，避免 .env 里的旧 key 失效
if command -v op &>/dev/null && [ -f ~/.openclaw/.op-token ]; then
  export OP_SERVICE_ACCOUNT_TOKEN=$(cat ~/.openclaw/.op-token 2>/dev/null)

  OP_DEEPSEEK_KEY=$(op read "op://Agent-All/deepseek-btc/password" 2>/dev/null)
  if [ -n "$OP_DEEPSEEK_KEY" ]; then
    export DEEPSEEK_KEY="$OP_DEEPSEEK_KEY"
  fi

  OP_FRED_KEY=$(op read "op://Agent-All/fred-api-key/password" 2>/dev/null)
  if [ -n "$OP_FRED_KEY" ]; then
    export FRED_KEY="$OP_FRED_KEY"
  fi
fi

cd "$SCRIPT_DIR/.."
python3 scripts/fetch_data.py >> /tmp/btc-fetch.log 2>&1
