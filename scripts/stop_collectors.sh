#!/usr/bin/env bash
# stop_collectors.sh — 停止 orderbook 守护进程 + 清除 cron 定时任务
# 用法: bash scripts/stop_collectors.sh

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LOGS="$REPO/logs"

# ---------------------------------------------------------------------------
# 1. 停止 orderbook 守护进程（含外层 restart loop）
# ---------------------------------------------------------------------------
ORDERBOOK_PID_FILE="$LOGS/orderbook.pid"

if [ -f "$ORDERBOOK_PID_FILE" ]; then
    PID=$(cat "$ORDERBOOK_PID_FILE")
    # Kill the entire process group (catches the subshell restart loop + child)
    if kill -0 "$PID" 2>/dev/null; then
        kill -- -"$PID" 2>/dev/null || kill "$PID" 2>/dev/null || true
        echo "[orderbook] Stopped (pid $PID)."
    else
        echo "[orderbook] Process $PID not running (already stopped)."
    fi
    rm -f "$ORDERBOOK_PID_FILE"
else
    echo "[orderbook] No pid file found — not running."
fi

# ---------------------------------------------------------------------------
# 2. 清除 cron 定时任务
# ---------------------------------------------------------------------------
CRON_TAG="# ai-hedge-fund collectors"
CURRENT_CRON=$(crontab -l 2>/dev/null || true)
STRIPPED=$(printf '%s\n' "$CURRENT_CRON" | awk "/$CRON_TAG/{found=1} !found{print} /$CRON_TAG end/{found=0}")

if printf '%s\n' "$STRIPPED" | crontab - 2>/dev/null; then
    echo "[cron] Removed ai-hedge-fund collector jobs."
else
    echo "[cron] WARNING: crontab write failed (macOS permission)."
    echo "[cron] To remove manually, run: crontab -e  and delete the ai-hedge-fund block."
fi

echo "Done."
