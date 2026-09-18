#!/usr/bin/env python3
"""Evaluate registered hypotheses — with the pre-registration discipline enforced.

This is the mechanical line the platform lacked: **an evaluation may only ever
touch data that post-dates the hypothesis's registration timestamp.**  A request
whose window starts before ``registered_at_utc`` is refused outright (exit code
2), and a request below the minimum sample size may return only "未达评估条件
（需 n≥X）" — never a verdict.

Usage::

    poetry run python scripts/evaluate_hypotheses.py \
        --registry hypotheses/registry.jsonl \
        --spec hypotheses/eval_requests.example.json \
        --out reports/registry_evaluation.json

Request spec (a list, or ``{"evaluations": [...]}``)::

    {
      "hypothesis_id": "some_new_signal",
      "data_start_utc": "2026-09-20T00:00:00Z",
      "data_end_utc":   "2026-10-20T00:00:00Z",
      "n": 45,
      "n_min": 30,
      "metric": "sharpe",
      "value": 0.9,
      "extra": {"notes": "..."}
    }

Exit codes: 0 = all evaluated, 1 = insufficient sample(s) only, 2 = a
pre-registration violation (hard failure).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.validation.registry import (  # noqa: E402
    DEFAULT_MIN_SAMPLE,
    PreRegistrationDataError,
    check_no_pre_registration_data,
    get_hypothesis,
    sample_sufficiency,
)

DEFAULT_REGISTRY = "hypotheses/registry.jsonl"
DEFAULT_OUT = "reports/registry_evaluation.json"


def evaluate_request(record: dict, request: dict) -> dict:
    """Evaluate one request against one registry record.

    Raises :class:`PreRegistrationDataError` if the request would use data from
    before the hypothesis was registered.  Otherwise returns a result dict whose
    ``status`` is ``EVALUATED`` or ``INSUFFICIENT_SAMPLE``.
    """
    # HARD DISCIPLINE: refuse pre-registration data before doing anything else.
    check_no_pre_registration_data(record, request["data_start_utc"])

    n_min = int(request.get("n_min", DEFAULT_MIN_SAMPLE))
    suff = sample_sufficiency(int(request.get("n", 0)), n_min=n_min)

    result: dict = {
        "hypothesis_id": record["hypothesis_id"],
        "registered_at_utc": record["registered_at_utc"],
        "data_start_utc": request["data_start_utc"],
        "data_end_utc": request.get("data_end_utc"),
        "n": suff["n"],
        "n_min": suff["n_min"],
        "sample": suff["message"],
        "metric": request.get("metric"),
        "value": request.get("value"),
        "resolution_criteria": record.get("resolution_criteria"),
    }

    if not suff["sufficient"]:
        result["status"] = "INSUFFICIENT_SAMPLE"
        result["verdict"] = None
        result["message"] = suff["message"]
        return result

    result["status"] = "EVALUATED"
    result["verdict"] = "EVALUATED"
    result["message"] = (
        f"样本充足（n={suff['n']} ≥ {suff['n_min']}）；"
        f"{request.get('metric')} = {request.get('value')}；"
        "是否满足 resolution_criteria 需按该条目的机械判据裁定。"
    )
    if request.get("extra"):
        result["extra"] = request["extra"]
    return result


def run(registry_path: str, requests: list[dict]) -> dict:
    """Run all requests, returning the report and a worst-case exit code."""
    results: list[dict] = []
    exit_code = 0
    for req in requests:
        hid = req.get("hypothesis_id")
        if hid is None:
            results.append({"hypothesis_id": None, "status": "BAD_REQUEST", "message": "missing hypothesis_id"})
            exit_code = max(exit_code, 2)
            continue
        record = get_hypothesis(registry_path, hid)
        if record is None:
            results.append({"hypothesis_id": hid, "status": "UNKNOWN_HYPOTHESIS", "message": f"'{hid}' not in registry"})
            exit_code = max(exit_code, 2)
            continue
        try:
            results.append(evaluate_request(record, req))
        except PreRegistrationDataError as exc:
            results.append(
                {
                    "hypothesis_id": hid,
                    "status": "PRE_REGISTRATION_REJECTED",
                    "message": str(exc),
                    "data_start_utc": req.get("data_start_utc"),
                    "registered_at_utc": record["registered_at_utc"],
                }
            )
            exit_code = 2

    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "registry": registry_path,
        "n_requests": len(requests),
        "n_evaluated": sum(1 for r in results if r["status"] == "EVALUATED"),
        "n_insufficient": sum(1 for r in results if r["status"] == "INSUFFICIENT_SAMPLE"),
        "n_pre_registration_rejected": sum(
            1 for r in results if r["status"] == "PRE_REGISTRATION_REJECTED"
        ),
        "rule": "an evaluation may only use data with data_start_utc >= registered_at_utc; "
                "below n_min only '未达评估条件' may be reported",
        "results": results,
        "exit_code": exit_code,
    }


def _load_requests(path: str) -> list[dict]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "evaluations" in data:
        return data["evaluations"]
    if isinstance(data, dict):
        return [data]
    raise ValueError(f"unsupported eval spec shape in {path}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--spec", required=True, help="JSON list of evaluation requests")
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    report = run(args.registry, _load_requests(args.spec))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    for r in report["results"]:
        hid = r.get("hypothesis_id")
        if r["status"] == "PRE_REGISTRATION_REJECTED":
            print(f"REFUSED {hid}: {r['message']}", file=sys.stderr)
        else:
            print(f"{r['status']:24s} {hid}: {r.get('message', '')}")
    print(
        f"\nwrote {args.out}  evaluated={report['n_evaluated']} "
        f"insufficient={report['n_insufficient']} "
        f"pre_registration_rejected={report['n_pre_registration_rejected']}"
    )
    return report["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
