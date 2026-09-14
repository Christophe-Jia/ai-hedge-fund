#!/usr/bin/env python3
"""Daily signal runner — combined weekend_gap + funding_rate, paper-trading v1.

Evaluates the two validated event signals for a given `as_of` date and
logs the result. v1 is REPORT ONLY — it never places orders; it prints a
suggested trade instruction that a human (or a later v2) can act on.

Signals (used read-only; see src/signals/ for implementations):
  - WeekendGapSignal : BTC weekend move >= 5% -> crypto-proxy stocks on Monday
  - FundingRateSignal: BTC perp funding extreme -> long crypto stocks
  - SignalCombiner   : equal-weight combination of the two

Decision rule (v1): |combined score| > 0.2 -> suggested trade with total
position size = |combined score| of equity, split equally across the
targets of the fired signals, entry at the next trading day open.
Otherwise: flat — no signal today, stay in cash / maintain position.

Data freshness: this script does NOT refresh data. Run
    poetry run python scripts/refresh_daily_data.py
first (in cron: refresh, then this runner). If data_health() reports
stale/unhealthy data the run still evaluates and records what it can,
but exits 1 so cron can alert.

Outputs:
  - stdout human-readable report (single conclusion line with --quiet)
  - reports/signals/YYYY-MM-DD.json — full structured snapshot of the day
  - reports/signals/log.jsonl       — one JSON line per day (the continuous
    paper-trading log for later win-rate / P&L stats). An existing line
    with the same date is replaced, so re-running a date is safe.

Usage:
    poetry run python scripts/run_daily_signals.py
    poetry run python scripts/run_daily_signals.py --as-of 2025-03-03
    poetry run python scripts/run_daily_signals.py --quiet

Exit codes:
    0 — evaluation completed on healthy data (flat is a valid outcome)
    1 — stale/unhealthy data or an evaluation error (cron-monitorable)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.signals import (
    FundingRateSignal,
    Signal,
    SignalCombiner,
    SignalOutput,
    WeekendGapSignal,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SIGNALS_DIR = PROJECT_ROOT / "reports" / "signals"
LOG_PATH = SIGNALS_DIR / "log.jsonl"

DECISION_THRESHOLD = 0.2
ENTRY_NOTE = "next trading day open"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _CachedSignal(Signal):
    """Serves a precomputed SignalOutput so each signal is evaluated once."""

    def __init__(self, name: str, outputs: dict) -> None:
        self.name = name
        self.description = ""
        self._outputs = outputs

    def generate(self, as_of: pd.Timestamp) -> SignalOutput | None:
        return self._outputs.get(pd.Timestamp(as_of).normalize())

    def data_health(self) -> dict:
        return {"signal": self.name, "ok": True, "cached": True}


def _utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()


def _parse_as_of(value: str) -> pd.Timestamp:
    try:
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            raise ValueError("NaT")
    except (ValueError, TypeError) as exc:
        raise argparse.ArgumentTypeError(
            f"invalid --as-of {value!r}: expected a date like 2025-03-03 ({exc})"
        ) from None
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.normalize()


def _output_dict(out: SignalOutput | None) -> dict | None:
    if out is None:
        return None
    return {
        "score": round(out.score, 4),
        "direction": out.direction,
        "confidence": round(out.confidence, 4),
        "metadata": out.metadata,
    }


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def evaluate_signals(
    signals: list[Signal], as_of: pd.Timestamp
) -> tuple[dict[str, SignalOutput | None], dict[str, str]]:
    """Run generate(as_of) on each signal; errors are captured, never fatal."""
    outputs: dict[str, SignalOutput | None] = {}
    errors: dict[str, str] = {}
    for sig in signals:
        try:
            outputs[sig.name] = sig.generate(as_of)
        except Exception as exc:  # noqa: BLE001 — record and continue
            outputs[sig.name] = None
            errors[sig.name] = str(exc)[:200]
    return outputs, errors


def collect_health(signals: list[Signal]) -> dict[str, dict]:
    health: dict[str, dict] = {}
    for sig in signals:
        try:
            health[sig.name] = sig.data_health()
        except Exception as exc:  # noqa: BLE001
            health[sig.name] = {"signal": sig.name, "ok": False, "error": str(exc)[:200]}
    return health


def build_decision(
    combined: SignalOutput | None,
    outputs: dict[str, SignalOutput | None],
) -> dict:
    """Map the combined score to a v1 trade instruction (or flat)."""
    fired = [name for name, out in outputs.items() if out is not None]
    if combined is None or abs(combined.score) <= DECISION_THRESHOLD:
        return {
            "action": "flat",
            "fired_signals": fired,
            "targets": [],
            "position_size": 0.0,
            "entry": None,
            "note": "no signal today; stay in cash / maintain current position",
        }

    targets: list[str] = []
    for name in fired:
        for sym in (outputs[name].metadata or {}).get("targets", []):
            if sym not in targets:
                targets.append(sym)
    return {
        "action": "long" if combined.score > 0 else "short",
        "fired_signals": fired,
        "targets": targets,
        "position_size": round(min(abs(combined.score), 1.0), 4),
        "entry": ENTRY_NOTE,
        "note": f"entry {ENTRY_NOTE}; split equally across targets",
    }


# ---------------------------------------------------------------------------
# Output files
# ---------------------------------------------------------------------------


def write_snapshot(path: Path, snapshot: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=2, default=str) + "\n")


def append_log_line(path: Path, entry: dict) -> bool:
    """Append one JSON line, replacing any existing line with the same date.

    Lines are kept sorted by date so the log stays a clean continuous
    record regardless of the order runs happen in (e.g. historical
    replays). Returns True if a new line was added, False if an existing
    date entry was updated in place.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    entries: list[dict] = []
    unparsable: list[str] = []
    replaced = False
    if path.exists():
        for raw in path.read_text().splitlines():
            if not raw.strip():
                continue
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                unparsable.append(raw)  # keep untouched
                continue
            if parsed.get("date") == entry["date"]:
                replaced = True
                continue  # drop the stale entry for this date
            entries.append(parsed)
    entries.append(entry)
    entries.sort(key=lambda e: str(e.get("date", "")))
    lines = [json.dumps(e, default=str) for e in entries] + unparsable
    path.write_text("\n".join(lines) + "\n")
    return not replaced


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _signal_detail(name: str, out: SignalOutput | None) -> str:
    if out is None:
        return "-"
    md = out.metadata or {}
    if name == "weekend_gap":
        return (
            f"BTC weekend {md.get('btc_weekend_return_pct', float('nan')):+.2f}% "
            f"({md.get('friday', '?')} -> {md.get('sunday', '?')}, "
            f"thr +/-{md.get('threshold_pct', '?')}%)"
        )
    if name == "funding_rate":
        return (
            f"funding 3d MA {md.get('funding_ma_annualized_pct', float('nan')):+.1f}% ann. "
            f"({md.get('regime', '?')} regime, new extreme: {md.get('is_new_extreme', '?')})"
        )
    return str(md)[:60] if md else "-"


def _health_summary(name: str, h: dict) -> str:
    if h.get("error"):
        return f"error: {h['error']}"
    parts: list[str] = []
    if name == "weekend_gap":
        btc = h.get("btc", {})
        if btc.get("last_date"):
            parts.append(
                f"btc last {btc['last_date']} ({btc.get('stale_days', '?')}d stale)"
            )
        elif btc.get("reason") or btc.get("error"):
            parts.append(f"btc: {btc.get('reason') or btc.get('error')}")
    if name == "funding_rate":
        f = h.get("funding", {})
        if f.get("last_event"):
            parts.append(f"funding last {f['last_event']} ({f.get('stale_hours', '?')}h stale)")
        elif f.get("reason") or f.get("error"):
            parts.append(f"funding: {f.get('reason') or f.get('error')}")
    stocks = h.get("stocks", {})
    if stocks:
        n_ok = sum(1 for v in stocks.values() if v.get("ok"))
        parts.append(f"stocks {n_ok}/{len(stocks)} fresh")
    return "; ".join(parts) if parts else str(h)[:80]


def print_report(
    as_of: pd.Timestamp,
    generated_at: str,
    signals: list[Signal],
    outputs: dict[str, SignalOutput | None],
    errors: dict[str, str],
    combined: SignalOutput | None,
    combiner_error: str | None,
    decision: dict,
    health: dict[str, dict],
    snapshot_path: Path | None,
    log_written: bool,
) -> None:
    sep = "-" * 80
    print("=" * 80)
    print(f"Daily Signals — as_of {as_of.date().isoformat()} (UTC)")
    print(f"generated: {generated_at} | paper trading v1 (report only, no orders)")
    print("=" * 80)

    print("\nSIGNALS")
    print(sep)
    print(f"{'signal':<15} {'triggered':<10} {'direction':<10} {'score':>6} {'conf':>6}  {'targets':<18} detail")
    for sig in signals:
        out = outputs[sig.name]
        err = errors.get(sig.name)
        if out is not None:
            targets = ",".join((out.metadata or {}).get("targets", []))
            print(
                f"{sig.name:<15} {'yes':<10} {out.direction:<10} "
                f"{out.score:>+6.2f} {out.confidence:>6.2f}  {targets:<18} {_signal_detail(sig.name, out)}"
            )
        elif err:
            print(f"{sig.name:<15} {'ERROR':<10} {'-':<10} {'-':>6} {'-':>6}  {'-':<18} {err}")
        else:
            print(
                f"{sig.name:<15} {'no':<10} {'-':<10} {'-':>6} {'-':>6}  {'-':<18} "
                "not triggered (below threshold / out of window)"
            )

    print("\nCOMBINED (equal weights)")
    print(sep)
    if combined is not None:
        md = combined.metadata or {}
        print(
            f"score {combined.score:+.2f} | direction {combined.direction} | "
            f"confidence {combined.confidence:.2f} | "
            f"contributing: {','.join(md.get('contributing', [])) or 'none'} | "
            f"missing: {','.join(md.get('missing', [])) or 'none'}"
        )
    else:
        print(f"combiner error: {combiner_error}")

    print(f"\nDECISION (|combined| > {DECISION_THRESHOLD} -> trade)")
    print(sep)
    if decision["action"] == "flat":
        print(f">>> ACTION: FLAT — {decision['note']}")
    else:
        n = len(decision["targets"]) or 1
        per = decision["position_size"] / n
        print(f">>> ACTION: {decision['action'].upper()} — suggested trade:")
        print(f"    targets:        {', '.join(decision['targets'])}")
        print(f"    position:       {decision['position_size']:.0%} of equity total (~{per:.0%} per target)")
        print(f"    entry:          {decision['entry']}")
        print(f"    fired signals:  {', '.join(decision['fired_signals'])}")

    print("\nDATA HEALTH")
    print(sep)
    all_ok = True
    for sig in signals:
        h = health[sig.name]
        ok = bool(h.get("ok"))
        all_ok = all_ok and ok
        print(f"{sig.name:<15} {'OK' if ok else 'FAIL':<6} {_health_summary(sig.name, h)}")
    if errors or combiner_error:
        all_ok = False
    if not all_ok:
        print("  hint: run `poetry run python scripts/refresh_daily_data.py` before this script.")

    if snapshot_path is not None:
        print(f"\nsnapshot: {snapshot_path.relative_to(PROJECT_ROOT)}")
        print(f"log:      reports/signals/log.jsonl ({'updated' if log_written else 'not written (unhealthy data)'})")
    print(f"\nOverall: {'OK' if all_ok else 'DEGRADED'} (exit {0 if all_ok else 1})")


def print_quiet(
    date_str: str,
    combined: SignalOutput | None,
    combiner_error: str | None,
    decision: dict,
) -> None:
    fired = ",".join(decision["fired_signals"]) or "none"
    if combiner_error or combined is None:
        print(f"{date_str} | combined ERROR ({combiner_error}) | fired: {fired} | ACTION: FLAT (degraded)")
        return
    if decision["action"] == "flat":
        print(f"{date_str} | combined {combined.score:+.2f} (flat) | fired: {fired} | ACTION: FLAT — no signal")
    else:
        print(
            f"{date_str} | combined {combined.score:+.2f} ({combined.direction}) | fired: {fired} | "
            f"ACTION: {decision['action'].upper()} {','.join(decision['targets'])} "
            f"@ {decision['position_size']:.0%} equity, entry {ENTRY_NOTE}"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--as-of",
        type=_parse_as_of,
        default=None,
        help="evaluate as of this UTC date (default: today); e.g. 2025-03-03",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="print only a one-line conclusion (cron-friendly); files are still written",
    )
    args = parser.parse_args(argv)

    as_of = args.as_of if args.as_of is not None else _utc_now()
    date_str = as_of.date().isoformat()
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    signals = [WeekendGapSignal(), FundingRateSignal()]

    outputs, errors = evaluate_signals(signals, as_of)

    # Combine over cached outputs so each signal is evaluated exactly once.
    combiner_error: str | None = None
    combined: SignalOutput | None = None
    try:
        cached = [_CachedSignal(s.name, {as_of: outputs[s.name]}) for s in signals]
        combined = SignalCombiner(cached, threshold=DECISION_THRESHOLD).combined_score(as_of)
    except Exception as exc:  # noqa: BLE001
        combiner_error = str(exc)[:200]

    health = collect_health(signals)
    decision = build_decision(combined, outputs)

    data_ok = (
        not errors
        and combiner_error is None
        and all(bool(h.get("ok")) for h in health.values())
    )

    # --- snapshot file (always written, even when degraded) ----------------
    snapshot = {
        "as_of": date_str,
        "generated_at": generated_at,
        "mode": "paper_trading_v1_report_only",
        "decision_threshold": DECISION_THRESHOLD,
        "signals": {
            s.name: {
                "triggered": outputs[s.name] is not None,
                "output": _output_dict(outputs[s.name]),
                "error": errors.get(s.name),
                "data_health": health[s.name],
            }
            for s in signals
        },
        "combined": _output_dict(combined),
        "combiner_error": combiner_error,
        "decision": decision,
        "data_ok": data_ok,
    }
    snapshot_path = SIGNALS_DIR / f"{date_str}.json"
    write_snapshot(snapshot_path, snapshot)

    # --- continuous log (only when healthy, so the record stays clean) -----
    log_written = False
    if data_ok:
        log_entry = {
            "date": date_str,
            "generated_at": generated_at,
            "combined_score": round(combined.score, 4) if combined else None,
            "combined_direction": combined.direction if combined else None,
            "fired_signals": decision["fired_signals"],
            "action": decision["action"],
            "targets": decision["targets"],
            "position_size": decision["position_size"],
        }
        log_written = append_log_line(LOG_PATH, log_entry)

    if args.quiet:
        print_quiet(date_str, combined, combiner_error, decision)
    else:
        print_report(
            as_of, generated_at, signals, outputs, errors,
            combined, combiner_error, decision, health,
            snapshot_path, log_written,
        )
    return 0 if data_ok else 1


if __name__ == "__main__":
    sys.exit(main())
