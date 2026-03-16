#!/bin/bash
# 一键启动 AI Hedge Fund Web App（前端 + 后端）
# 用法：./run.sh
# 访问：http://localhost:5173

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR/app" && exec ./run.sh "$@"
