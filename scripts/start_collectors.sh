#!/usr/bin/env bash
# start_collectors.sh — 启动全部采集守护进程 + 注册 cron 定时任务
#
# 用法:
#   bash scripts/start_collectors.sh          # 启动 orderbook + 注册 cron
#   bash scripts/start_collectors.sh --no-cron  # 只启动 orderbook，跳过 cron
#
# 守护进程:
#   orderbook (WebSocket 常驻) → logs/orderbook.log  (断线自动重连)
#
# Cron 定时任务:
#   每小时    collect_crypto_data.py    → logs/collect_crypto.log
#   每 6 小时 backfill_perp_ohlcv.py   → logs/backfill_perp.log
#   每天 02:00 collect_macro_data.py   → logs/collect_macro.log
#   每天 03:00 backfill_onchain.py     → logs/backfill_onchain.log
#
# macOS 注意: 若 crontab 写入失败（Operation not permitted），请在
#   系统设置 → 隐私与安全性 → 完全磁盘访问 中为终端授权，然后重新运行。
#   或者直接手动执行: crontab scripts/crontab.txt

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LOGS="$REPO/logs"
POETRY_BIN="$(which poetry)"
NO_CRON=false

for arg in "$@"; do
    [ "$arg" = "--no-cron" ] && NO_CRON=true
done

mkdir -p "$LOGS"

# ---------------------------------------------------------------------------
# 1. orderbook 守护进程（WebSocket 常驻，断线自动重启）
# ---------------------------------------------------------------------------
ORDERBOOK_PID_FILE="$LOGS/orderbook.pid"

if [ -f "$ORDERBOOK_PID_FILE" ]; then
    OLD_PID=$(cat "$ORDERBOOK_PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "[orderbook] Already running (pid $OLD_PID) — skip."
    else
        echo "[orderbook] Stale pid file, restarting..."
        rm -f "$ORDERBOOK_PID_FILE"
    fi
fi

if [ ! -f "$ORDERBOOK_PID_FILE" ]; then
    # 外层 while 循环：脚本因任何原因退出后自动重启
    (
        while true; do
            "$POETRY_BIN" -C "$REPO" run python "$REPO/scripts/collect_orderbook.py" \
                --symbols BTC/USDT,ETH/USDT --exchange gate \
                >> "$LOGS/orderbook.log" 2>&1
            echo "[orderbook] exited at $(date -u +%Y-%m-%dT%H:%M:%SZ), restarting in 10s..." \
                >> "$LOGS/orderbook.log"
            sleep 10
        done
    ) &
    echo $! > "$ORDERBOOK_PID_FILE"
    echo "[orderbook] Started (pid $(cat "$ORDERBOOK_PID_FILE")) → logs/orderbook.log"
fi

# ---------------------------------------------------------------------------
# 2. Cron 定时任务
# ---------------------------------------------------------------------------
if [ "$NO_CRON" = true ]; then
    echo "[cron] Skipped (--no-cron)."
    echo ""
    echo "Done. Run 'bash scripts/stop_collectors.sh' to stop."
    exit 0
fi

CRON_TAG="# ai-hedge-fund collectors"
NEW_CRON="$CRON_TAG
# every hour: crypto OHLCV snapshots
0 * * * * cd $REPO && $POETRY_BIN run python scripts/collect_crypto_data.py >> $LOGS/collect_crypto.log 2>&1
# every 6 hours: perp OHLCV + funding rate backfill
15 */6 * * * cd $REPO && $POETRY_BIN run python scripts/backfill_perp_ohlcv.py >> $LOGS/backfill_perp.log 2>&1
# daily 02:00 UTC: macro data (DXY, GOLD, VIX, TNX)
0 2 * * * cd $REPO && $POETRY_BIN run python scripts/collect_macro_data.py >> $LOGS/collect_macro.log 2>&1
# daily 03:00 UTC: onchain metrics (CoinGecko, incremental)
0 3 * * * cd $REPO && $POETRY_BIN run python scripts/backfill_onchain.py --incremental >> $LOGS/backfill_onchain.log 2>&1
$CRON_TAG end"

# 同时写一份 crontab.txt 供手动导入（crontab 权限不足时的备用方案）
CRON_FILE="$REPO/scripts/crontab.txt"
cat > "$CRON_FILE" <<HEREDOC
$NEW_CRON
HEREDOC
echo "[cron] Saved cron definitions → scripts/crontab.txt"

# 尝试自动写入 crontab
if crontab -l > /dev/null 2>&1 || true; then
    CURRENT_CRON=$(crontab -l 2>/dev/null || true)
    STRIPPED=$(printf '%s\n' "$CURRENT_CRON" | awk "/$CRON_TAG/{found=1} !found{print} /$CRON_TAG end/{found=0}")
    if (printf '%s\n' "$STRIPPED"; printf '%s\n' "$NEW_CRON") | crontab - 2>/dev/null; then
        echo "[cron] Registered jobs:"
        crontab -l | grep -A20 "$CRON_TAG" | grep -v "^$CRON_TAG"
    else
        echo "[cron] WARNING: crontab write failed (macOS permission)."
        echo "[cron] To register manually, run:"
        echo "         crontab scripts/crontab.txt"
        echo "       Or: System Settings → Privacy → Full Disk Access → enable Terminal, then rerun."
    fi
fi

echo ""
echo "Done. Run 'bash scripts/stop_collectors.sh' to stop everything."
