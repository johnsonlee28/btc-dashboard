#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

# 先加载.env
if [ -f "$ENV_FILE" ]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

# 如果DEEPSEEK_KEY为空，从1Password读
if [ -z "$DEEPSEEK_KEY" ] && command -v op &>/dev/null; then
  export OP_SERVICE_ACCOUNT_TOKEN=$(cat ~/.openclaw/.op-token 2>/dev/null)
  export DEEPSEEK_KEY=$(op read "op://Agent-All/deepseek-key/password" 2>/dev/null)
fi

cd "$SCRIPT_DIR/.."
python3 scripts/fetch_data.py >> /tmp/btc-fetch.log 2>&1
