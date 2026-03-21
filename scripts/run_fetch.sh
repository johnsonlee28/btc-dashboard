#!/bin/bash
# 从 .env 加载密钥（.env 不会被 push 到 GitHub）
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
  export $(grep -v '^#' "$ENV_FILE" | xargs)
fi
cd "$SCRIPT_DIR/.."
python3 scripts/fetch_data.py >> /tmp/btc-fetch.log 2>&1
