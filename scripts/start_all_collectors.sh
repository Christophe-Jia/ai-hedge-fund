#!/usr/bin/env bash
# start_all_collectors.sh — 一键拉起三个数据采集守护进程（Mac 重启后恢复采集用）
#
# 采集器（常驻轮询，默认参数即生产配置，均写各自 SQLite、断线自动重试）:
#   collect_bbo_snapshots.py     Gate 现货/永续订单簿 top20 + 现货成交快照
#                                → logs/bbo_snapshots.log    (data/bbo_snapshots.db)
#   collect_polymarket_ticks.py  Polymarket CLOB 价格 tick（tag 发现 + 死市场退役）
#                                → logs/polymarket_ticks.log (data/polymarket_ticks.db)
#   collect_deriv_snapshots.py   Gate 永续 OI / 多空比 / 清算窗口聚合
#                                → logs/deriv_snapshots.log  (data/deriv_snapshots.db)
#
# 用法:
#   bash scripts/start_all_collectors.sh
#   ./scripts/start_all_collectors.sh
#
# 已在运行的采集器自动跳过（pgrep -f 按脚本路径检查），因此本脚本可放心重复执行。
# 停止单个: pkill -f collect_bbo_snapshots.py （其余同名替换）

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LOGS="$REPO/logs"
mkdir -p "$LOGS"

if ! command -v poetry > /dev/null 2>&1; then
    echo "[ERROR] poetry not found in PATH — collectors must run via 'poetry run'." >&2
    exit 1
fi

# name|script|log
COLLECTORS=(
    "bbo|collect_bbo_snapshots.py|bbo_snapshots.log"
    "polymarket|collect_polymarket_ticks.py|polymarket_ticks.log"
    "deriv|collect_deriv_snapshots.py|deriv_snapshots.log"
)

STARTED=0
SKIPPED=0

for entry in "${COLLECTORS[@]}"; do
    name="${entry%%|*}"
    rest="${entry#*|}"
    script="${rest%%|*}"
    log="${rest#*|}"

    if pgrep -f "scripts/${script}" > /dev/null 2>&1; then
        pid="$(pgrep -f "scripts/${script}" | head -1)"
        echo "[${name}] already running (pid ${pid}) — skip"
        SKIPPED=$((SKIPPED + 1))
    else
        nohup poetry run python "${REPO}/scripts/${script}" \
            >> "${LOGS}/${log}" 2>&1 &
        echo "[${name}] started (pid $!) → logs/${log}"
        STARTED=$((STARTED + 1))
        sleep 1   # 间隔启动，避免同时打库/抢锁
    fi
done

echo ""
echo "=== collector status ==="
for entry in "${COLLECTORS[@]}"; do
    name="${entry%%|*}"
    rest="${entry#*|}"
    script="${rest%%|*}"
    log="${rest#*|}"
    if pgrep -f "scripts/${script}" > /dev/null 2>&1; then
        pid="$(pgrep -f "scripts/${script}" | head -1)"
        last="$(tail -n 1 "${LOGS}/${log}" 2>/dev/null | cut -c1-90)"
        printf "  %-11s RUNNING  pid %-7s %s\n" "${name}" "${pid}" "${last}"
    else
        printf "  %-11s NOT RUNNING (check logs/%s)\n" "${name}" "${log}"
    fi
done
echo "started: ${STARTED}, skipped (already running): ${SKIPPED}"
