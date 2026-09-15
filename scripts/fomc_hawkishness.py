#!/usr/bin/env python3
"""FOMC statement hawkishness scoring, 2016-2026.

Flagship "LLM in the quant pipeline" experiment, step 1: score the hawkishness
of every FOMC statement and test whether TEXT carries increment information
beyond the observed rate decision (hike/cut/hold).

Known prior (reports/fomc_effect.json): FOMC decision-day effects on
crypto-concept stocks were a 2020-22 ZIRP artifact, gone after 2023. The text
dimension is held to the same standard: mandatory era split, honest negatives.

Scoring sources:
- lexicon (default, v1): transparent phrase + word list with fixed weights,
  applied to the statement body (excl. voting/dissent section). Academic
  standard approach (cf. Apel & Blix 2014, Hansen & McMahon 2016).
- api: any OpenAI-compatible chat endpoint (env: FOMC_LLM_BASE_URL,
  FOMC_LLM_API_KEY, FOMC_LLM_MODEL). Temperature 0, strict JSON output,
  per-statement independent, cached. Added AFTER network/key probe showed no
  usable key in this environment (sk-kimi key rejected by Moonshot endpoints;
  everything else in .env is a placeholder) -- kept as the upgrade path.

Outputs:
- data/fomc_statements/YYYYMMDD.txt  (statement bodies; /data/ is gitignored)
- reports/fomc_hawkishness.json      (scores, sanity, correlations, eras)

Usage:
    poetry run python scripts/fomc_hawkishness.py --step all            # default lexicon
    poetry run python scripts/fomc_hawkishness.py --step fetch
    poetry run python scripts/fomc_hawkishness.py --step score --source api
    poetry run python scripts/fomc_hawkishness.py --step analyze

FOMC calendar is imported read-only from scripts/backtest_fomc_effect.py.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as sps

SCRIPTS = Path(__file__).resolve().parent
ROOT = SCRIPTS.parent
sys.path.insert(0, str(SCRIPTS))
from backtest_fomc_effect import FOMC_EVENTS  # noqa: E402  (read-only import)

STATEMENTS_DIR = ROOT / "data" / "fomc_statements"
OUT = ROOT / "reports" / "fomc_hawkishness.json"
DB = ROOT / "data" / "btc_history.db"

STOCKS = ["MSTR", "MARA", "RIOT", "COIN"]
ERAS = {"2016-2019": (2016, 2019), "2020-2022": (2020, 2022), "2023-2026": (2023, 2026)}

# The reference calendar (backtest_fomc_effect.py) lists 2019-04-30, the FIRST
# day of the Apr 30 - May 1 2019 meeting; the decision/statement day is
# 2019-05-01 (verified: monetary20190501a.htm exists, monetary20190430a.htm 404).
DATE_OVERRIDES = {"2019-04-30": "2019-05-01"}

# ---------------------------------------------------------------------------
# Fetch: federalreserve.gov statement pages -> plain text
# ---------------------------------------------------------------------------
URL = "https://www.federalreserve.gov/newsevents/pressreleases/monetary{ymd}a.htm"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) research-script/1.0"


def html_to_text(html: str) -> str | None:
    m = re.search(r'<div class="col-xs-12 col-sm-8 col-md-8">(.*)', html, re.S)
    seg = m.group(1) if m else html
    seg = re.sub(r"<script.*?</script>", " ", seg, flags=re.S)
    seg = re.sub(r"<[^>]+>", " ", seg)
    seg = seg.replace("&#8217;", "\u2019").replace("&#8212;", "\u2014")
    seg = re.sub(r"&nbsp;|&#160;", " ", seg)
    seg = seg.replace("&amp;", "&").replace("&frac12;", "1/2")
    seg = re.sub(r"\s+", " ", seg).strip()
    # drop leading release-time banner if present ("For release at 2:00 p.m. ...")
    seg = re.sub(r"^For release at .*?\.m\.\s*[A-Z]\.?[A-Z]?\.?\s*", "", seg, flags=re.I)
    # statement body ends where the voting section begins
    for cut in ("Voting for the", "Voting against the"):
        i = seg.find(cut)
        if i > 0:
            seg = seg[:i].strip()
    # also strip trailing footer junk if voting section absent
    for tail in ("For media inquiries", "Last Update:", "Implementation Note issued"):
        i = seg.find(tail)
        if i > 0:
            seg = seg[:i].strip()
    return seg or None


def fetch_all(sleep_s: float = 1.5) -> None:
    STATEMENTS_DIR.mkdir(parents=True, exist_ok=True)
    ok, missing = [], []
    for (d, *_rest) in FOMC_EVENTS:
        d = DATE_OVERRIDES.get(d, d)
        ymd = d.replace("-", "")
        path = STATEMENTS_DIR / f"{ymd}.txt"
        if path.exists() and path.stat().st_size > 200:
            ok.append(d)
            continue
        url = URL.format(ymd=ymd)
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                html = r.read().decode("utf-8", errors="replace")
            if r.status != 200 or "monetary policy" not in html.lower():
                raise ValueError(f"unexpected page (status {r.status})")
            txt = html_to_text(html)
            if not txt or len(txt) < 300:
                raise ValueError("extraction failed / too short")
            path.write_text(txt)
            print(f"  {d}  ok  ({len(txt)} chars)")
            ok.append(d)
        except Exception as e:  # noqa: BLE001  (record and move on)
            missing.append((d, str(e)[:80]))
            print(f"  {d}  MISSING ({e})")
        time.sleep(sleep_s)
    print(f"\nfetched/verified {len(ok)}, missing {len(missing)}: {missing}")


# ---------------------------------------------------------------------------
# Lexicon scoring (transparent, auditable)
# ---------------------------------------------------------------------------
# Step 1: multi-word phrases are replaced by canonical tokens (so e.g.
# "reducing its holdings" counts hawkish, not dovish-unigram "reducing").
PHRASES = [
    (r"reduc\w*\s+(?:its|the)\s+(?:holdings|balance\s+sheet)", "PH_BS_RUNOFF", 2.0),
    (r"balances?\s*sheet\s+runoff", "PH_BS_RUNOFF", 2.0),
    (r"\brunoff\b", "PH_BS_RUNOFF", 2.0),
    (r"(?:increase|increas\w*|expand\w*)\s+(?:its|the)\s+(?:holdings|balance\s+sheet)",
     "PH_BS_EXPAND", -2.0),
    (r"(?:reduce|reduc\w*|slow\w*|decrease|decreas\w*)\s+the\s+pace", "PH_TAPER", 2.0),
    (r"(?:increase|increas\w*)\s+the\s+pace", "PH_QE", -2.0),
    (r"purchas\w*\s+(?:of\s+)?(?:Treasury|agency|additional|mortgage)", "PH_QE", -2.0),
    (r"\btaper(?:ing)?\b", "PH_TAPER", 2.0),
    (r"downside\s+risks?", "PH_RISK_DOWN", -2.0),
    (r"upside\s+risks?", "PH_RISK_UP", 2.0),
    (r"act\s+as\s+appropriate", "PH_ACCOM_BIAS", -1.0),
    (r"sustain\s+the\s+expansion", "PH_ACCOM_BIAS", -1.0),
    (r"\btransitory\b", "PH_TRANSITORY", -1.0),
    (r"\bpatient\b|\bpatience\b", "PH_PATIENT", -1.0),
    (r"considerable\s+time", "PH_ACCOM_BIAS", -1.0),
]

# Step 2: unigram weights on the phrase-substituted text (canonical tokens
# carry their own weight and are consumed by the substitution).
HAWKISH = {
    2.0: ["raise", "raises", "raised", "raising", "hike", "hikes", "hiked",
          "hiking", "tighten", "tightens", "tightened", "tightening",
          "firm", "firms", "firmed", "firming"],
    1.0: ["increase", "increases", "increased", "increasing", "strong",
          "stronger", "strongly", "strengthen", "strengthened",
          "strengthening", "elevated", "rapidly", "rapid", "faster",
          "accelerate", "accelerated", "accelerating", "upside"],
}
DOVISH = {
    -2.0: ["cut", "cuts", "lower", "lowers", "lowered", "lowering", "reduce",
           "reduces", "reduced", "reducing", "ease", "eases", "eased",
           "easing", "accommodative", "accommodation"],
    -1.0: ["weak", "weaker", "weaken", "weakened", "weakening", "weakness",
           "weaknesses", "slowed", "slowing", "soft", "softer", "softening",
           "moderated", "moderating", "downside", "diminished",
           "deteriorate", "deteriorated", "deteriorating"],
}
PHRASE_TOKEN_WEIGHT = {
    "PH_BS_RUNOFF": 2.0, "PH_TAPER": 2.0, "PH_RISK_UP": 2.0,
    "PH_BS_EXPAND": -2.0, "PH_QE": -2.0, "PH_RISK_DOWN": -2.0,
    "PH_ACCOM_BIAS": -1.0, "PH_TRANSITORY": -1.0, "PH_PATIENT": -1.0,
}

UNIGRAM_WEIGHT = {w: wt for wt, ws in HAWKISH.items() for w in ws}
UNIGRAM_WEIGHT.update({w: wt for wt, ws in DOVISH.items() for w in ws})


def score_lexicon(text: str) -> dict:
    """Return raw net weight, per-token hits, and normalized score in [-1,1]."""
    t = text.lower()
    hits: dict[str, float] = {}
    raw = 0.0
    for pat, token, _w in PHRASES:
        n = len(re.findall(pat, t, flags=re.I))
        if n:
            t = re.sub(pat, " ", t, flags=re.I)  # consume matched words
            hits[token] = hits.get(token, 0.0) + n
            raw += n * PHRASE_TOKEN_WEIGHT[token]
    for tok in t.replace("\u2011", "-").split():
        tok = tok.strip(".,;:()'\"")
        if tok in UNIGRAM_WEIGHT:
            hits[tok] = hits.get(tok, 0.0) + 1
            raw += UNIGRAM_WEIGHT[tok]
    return {
        "raw": raw,
        "normalized": round(float(np.tanh(raw / 8.0)), 4),
        "hits": {k: v for k, v in hits.items() if v},
    }


# ---------------------------------------------------------------------------
# LLM scoring (OpenAI-compatible endpoint, cached, temperature 0)
# ---------------------------------------------------------------------------
LLM_SYSTEM = (
    "You are a central-bank tone analyst. Score the hawkishness of the FOMC "
    "statement text on a scale from -1 (maximally dovish: easing, cutting, "
    "maximum accommodation) to +1 (maximally hawkish: tightening, hiking, "
    "inflation fighting). Judge the FULL text: rate action, forward guidance, "
    "balance-sheet language, risk assessment. Respond ONLY with JSON: "
    '{"score": <float in [-1,1]>, "reason": "<one sentence>"}'
)


def llm_score_one(text: str) -> dict | None:
    base = os.environ.get("FOMC_LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    key = os.environ.get("FOMC_LLM_API_KEY", os.environ.get("OPENAI_API_KEY", ""))
    model = os.environ.get("FOMC_LLM_MODEL", "gpt-4o-mini")
    if not key:
        raise SystemExit("FOMC_LLM_API_KEY / OPENAI_API_KEY not set -- cannot use --source api")
    body = json.dumps({
        "model": model,
        "temperature": 0,
        "max_tokens": 200,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": LLM_SYSTEM},
            {"role": "user", "content": text[:12000]},
        ],
    }).encode()
    req = urllib.request.Request(
        f"{base}/chat/completions", data=body,
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        out = json.loads(r.read())
    content = out["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    return {
        "raw": None,
        "normalized": float(np.clip(float(parsed["score"]), -1.0, 1.0)),
        "reason": str(parsed.get("reason", ""))[:300],
        "model": model,
    }


LLM_CACHE = STATEMENTS_DIR / ".llm_scores_cache.json"


def score_all(source: str) -> list[dict]:
    rows = []
    cache = {}
    if source == "api" and LLM_CACHE.exists():
        cache = json.loads(LLM_CACHE.read_text())
    prev = None
    for (d, action, bp, sched, _dc, _ac, note) in FOMC_EVENTS:
        d = DATE_OVERRIDES.get(d, d)
        ymd = d.replace("-", "")
        path = STATEMENTS_DIR / f"{ymd}.txt"
        if not path.exists():
            continue  # future meetings / unfetchable
        text = path.read_text()
        if source == "api":
            if d in cache:
                sc = cache[d]
            else:
                sc = llm_score_one(text)
                cache[d] = sc
                LLM_CACHE.write_text(json.dumps(cache, indent=1))
                time.sleep(1.0)
        else:
            sc = score_lexicon(text)
        row = {
            "date": d, "action": action, "bp": bp, "scheduled": sched,
            "note": note, "score_raw": sc["raw"], "score": sc["normalized"],
        }
        if source == "api":
            row["llm_reason"] = sc.get("reason", "")
        else:
            row["lexicon_hits"] = sc["hits"]
        row["delta"] = round(row["score"] - prev, 4) if prev is not None else None
        prev = row["score"]
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
def load_daily(symbol: str, market_type: str) -> pd.DataFrame:
    import sqlite3
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    df = pd.read_sql(
        "SELECT ts, close FROM ohlcv WHERE symbol=? AND market_type=? "
        "AND timeframe='1d'", con, params=(symbol, market_type))
    con.close()
    df["date"] = pd.to_datetime(df["ts"], unit="ms").dt.normalize()
    return df.set_index("date").sort_index()["close"]


def fwd_ret(px: pd.Series, d: pd.Timestamp, horizon: int) -> float | None:
    """Close-to-close forward return over `horizon` bars of this series.

    Event day maps to the first quoted date >= d (handles weekends/holidays).
    Bars = trading days for stocks/ETF, calendar days for BTC (same convention
    as backtest_fomc_effect.py).
    """
    i = px.index.searchsorted(d)
    j = i + horizon
    if i >= len(px) or j >= len(px):
        return None
    p0, p1 = px.iloc[i], px.iloc[j]
    if pd.isna(p0) or pd.isna(p1) or p0 <= 0:
        return None
    return float(p1 / p0 - 1.0) * 100.0


def corr_cells(x: list[float], y: list[float]) -> dict:
    if len(x) < 4:
        return {"n": len(x)}
    pearson_r, pearson_p = sps.pearsonr(x, y)
    spearman_r, spearman_p = sps.spearmanr(x, y)
    return {
        "n": len(x),
        "pearson_r": round(float(pearson_r), 3),
        "pearson_p": round(float(pearson_p), 4),
        "spearman_r": round(float(spearman_r), 3),
        "spearman_p": round(float(spearman_p), 4),
    }


def analyze(rows: list[dict], source: str) -> None:
    # ----- prices -----
    px = {s: load_daily(s, "stocks") for s in STOCKS}
    px["QQQ"] = load_daily("QQQ", "etf")
    px["BTC"] = load_daily("BTC/USDT", "spot")
    # equal-weight crypto basket price (rebased mean of normalized prices)
    base = {s: px[s] / px[s].iloc[0] for s in STOCKS}
    basket_px = pd.concat(base.values(), axis=1).mean(axis=1, skipna=True)
    px["crypto_basket"] = basket_px

    # forward returns per event
    for row in rows:
        d = pd.Timestamp(row["date"])
        for h in (1, 5):
            for name in ("crypto_basket", "QQQ", "BTC"):
                v = fwd_ret(px[name], d, h)
                row[f"{name}_T+{h}"] = round(v, 3) if v is not None else None

    scored = [r for r in rows if r["delta"] is not None]

    # ----- 1. trajectory sanity -----
    df = pd.DataFrame(scored)
    df["year"] = df["date"].str[:4].astype(int)
    yearly = df.groupby("year")["score"].agg(["mean", "count"]).round(3)
    cycle = {
        "expected_narrative": [
            "2016-2018: hiking cycle -> rising positive scores",
            "2019: mid-year dovish pivot -> falling scores",
            "2020-2021: ZIRP + QE -> deeply negative floor",
            "2022-2023: inflation fight -> strongest positive scores",
            "2024-2025: cutting cycle -> falling to negative",
        ],
        "yearly_mean_score": {
            str(y): {"mean": float(v["mean"]), "n": int(v["count"])}
            for y, v in yearly.iterrows()
        },
    }
    # score vs signed bp (decision content check)
    with_bp = [r for r in scored if r["bp"] is not None]
    cycle["score_vs_bp_change"] = corr_cells(
        [r["score"] for r in with_bp],
        [float(r["bp"]) for r in with_bp])
    # delta vs bp: is the tone SHIFT just re-encoding the decision?
    cycle["delta_vs_bp_change"] = corr_cells(
        [r["delta"] for r in with_bp],
        [float(r["bp"]) for r in with_bp])

    # ----- 2. hawkish delta vs forward returns -----
    def era_of(d: str) -> str | None:
        for name, (y0, y1) in ERAS.items():
            if y0 <= int(d[:4]) <= y1:
                return name
        return None

    assets = ["crypto_basket", "QQQ", "BTC"]
    delta_corr = {}
    for name in assets:
        cells = {}
        xs_all, ys_all = [], []
        for h in (1, 5):
            xs = [r["delta"] for r in scored if r.get(f"{name}_T+{h}") is not None]
            ys = [r[f"{name}_T+{h}"] for r in scored if r.get(f"{name}_T+{h}") is not None]
            cells[f"T+{h}_all"] = corr_cells(xs, ys)
            xs_all += xs
            ys_all += ys
            for ename in ERAS:
                sub = [r for r in scored if era_of(r["date"]) == ename
                       and r.get(f"{name}_T+{h}") is not None]
                cells[f"T+{h}_{ename}"] = corr_cells(
                    [r["delta"] for r in sub], [r[f"{name}_T+{h}"] for r in sub])
        cells["pooled_T+1_and_T+5"] = corr_cells(xs_all, ys_all)
        delta_corr[name] = cells

    # ----- 3. decision-controlled: text delta within action buckets -----
    controlled = {}
    for action in ("hold", "hike", "cut"):
        sub = [r for r in scored if r["action"] == action]
        cells = {"n": len(sub)}
        for name in assets:
            for h in (1, 5):
                xs = [r["delta"] for r in sub if r.get(f"{name}_T+{h}") is not None]
                ys = [r[f"{name}_T+{h}"] for r in sub if r.get(f"{name}_T+{h}") is not None]
                cells[f"{name}_T+{h}"] = corr_cells(xs, ys)
        controlled[action] = cells

    # joint regression: ret ~ hike_dummy + cut_dummy + delta (does text survive?)
    regressions = {}
    for name in assets:
        for h in (1, 5):
            sub = [r for r in scored if r.get(f"{name}_T+{h}") is not None
                   and r["bp"] is not None]
            if len(sub) < 10:
                continue
            X = np.column_stack([
                np.ones(len(sub)),
                [1.0 if r["action"] == "hike" else 0.0 for r in sub],
                [1.0 if r["action"] == "cut" else 0.0 for r in sub],
                [r["delta"] for r in sub],
            ])
            y = np.array([r[f"{name}_T+{h}"] for r in sub])
            beta, *_ = np.linalg.lstsq(X, y, rcond=None)
            resid = y - X @ beta
            dof = len(sub) - X.shape[1]
            if dof <= 0:
                continue
            s2 = float(resid @ resid) / dof
            cov = s2 * np.linalg.pinv(X.T @ X)
            se = np.sqrt(np.diag(cov))
            regressions[f"{name}_T+{h}"] = {
                "n": len(sub),
                "beta_delta": round(float(beta[3]), 3),
                "se_delta": round(float(se[3]), 3),
                "t_delta": round(float(beta[3] / se[3]), 2) if se[3] > 0 else None,
                "beta_hike_dummy": round(float(beta[1]), 3),
                "beta_cut_dummy": round(float(beta[2]), 3),
            }

    # ----- 4. tercile sanity: most-dovish-shift vs most-hawkish-shift events -----
    terciles = {}
    if len(scored) >= 9:
        ds = sorted(scored, key=lambda r: r["delta"])
        k = len(ds) // 3
        for label, part in (("most_dovish_shift", ds[:k]),
                            ("middle", ds[k:-k] or ds[k:k + 1]),
                            ("most_hawkish_shift", ds[-k:])):
            terciles[label] = {
                name: {
                    f"T+{h}": round(float(np.mean([r[f"{name}_T+{h}"] for r in part
                                                    if r.get(f"{name}_T+{h}") is not None])), 3)
                    if any(r.get(f"{name}_T+{h}") is not None for r in part) else None
                    for h in (1, 5)
                } for name in assets
            }
            terciles[label]["n"] = len(part)

    # ----- assemble -----
    report = {
        "meta": {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "source": source,
            "llm_availability_probe": (
                "network: api.openai.com & api.anthropic.com reachable (401/405); "
                "keys: OPENAI_API_KEY=sk-kimi-* rejected by api.moonshot.ai/.cn "
                "(Invalid Authentication); all other .env keys are 'your-*' "
                "placeholders -> lexicon v1 used; api path kept via "
                "FOMC_LLM_BASE_URL/FOMC_LLM_API_KEY/FOMC_LLM_MODEL env vars"),
            "lexicon": {
                "phrases": [(p, t, PHRASE_TOKEN_WEIGHT[t]) for p, t, _ in PHRASES],
                "hawkish_unigrams": HAWKISH,
                "dovish_unigrams": DOVISH,
                "normalization": "tanh(raw/8), raw = weighted match sum; "
                                 "statement body only (voting/dissent section excluded)",
            },
            "n_statements_scored": len(rows),
            "n_with_delta": len(scored),
            "multiple_comparison_warning": (
                "3 assets x 2 horizons x (all + 3 eras + 3 action buckets) = ~100 "
                "correlation cells; isolated 'significant' cells are noise unless "
                "consistent across adjacent horizons/assets"),
            "prior_result": "reports/fomc_effect.json: decision-day effects were a "
                            "2020-22 artifact, gone 2023+; text held to same era-split standard",
        },
        "scores": rows,
        "trajectory_sanity": cycle,
        "delta_vs_forward_returns": delta_corr,
        "decision_controlled_within_action": controlled,
        "regression_text_beyond_decision": regressions,
        "delta_terciles": terciles,
    }
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2, default=str))
    print(f"wrote {OUT}")

    # ----- console summary -----
    print("\n=== Yearly mean score (expect: 16-18 up, 19 down, 20-21 floor, "
          "22-23 peak, 24-25 down) ===")
    for y, v in cycle["yearly_mean_score"].items():
        bar = "#" * int(abs(v["mean"]) * 40)
        print(f"  {y}  {v['mean']:+.3f} (n={v['n']})  {'+' if v['mean']>=0 else '-'}{bar}")
    print(f"  score vs signed bp: r={cycle['score_vs_bp_change'].get('pearson_r')}")

    print("\n=== Delta vs forward returns (pearson r / spearman r / n) ===")
    for name in assets:
        for h in (1, 5):
            c = delta_corr[name][f"T+{h}_all"]
            if c.get("n", 0) >= 4:
                print(f"  {name:14s} T+{h}: pr={c['pearson_r']:+.3f} "
                      f"sr={c['spearman_r']:+.3f} n={c['n']}")
        for ename in ERAS:
            c = delta_corr[name].get(f"T+1_{ename}", {})
            if c.get("n", 0) >= 4:
                print(f"    {ename}: pr={c['pearson_r']:+.3f} "
                      f"sr={c['spearman_r']:+.3f} n={c['n']}")

    print("\n=== Decision-controlled (hold bucket, delta vs T+1) ===")
    for name in assets:
        c = controlled["hold"].get(f"{name}_T+1", {})
        if c.get("n", 0) >= 4:
            print(f"  hold {name:14s}: pr={c['pearson_r']:+.3f} "
                  f"sr={c['spearman_r']:+.3f} n={c['n']}")

    print("\n=== Regression: ret ~ hike + cut + delta (t of delta) ===")
    for k, v in regressions.items():
        print(f"  {k:22s} beta_delta={v['beta_delta']:+.3f} "
              f"t={v['t_delta']:+.2f} n={v['n']}")


# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--step", choices=["fetch", "score", "analyze", "all"],
                    default="all")
    ap.add_argument("--source", choices=["lexicon", "api"], default="lexicon")
    args = ap.parse_args()

    if args.step in ("fetch", "all"):
        print(f"--- fetch ({len(FOMC_EVENTS)} events on calendar) ---")
        fetch_all()
    if args.step in ("score", "analyze", "all"):
        print(f"--- score (source={args.source}) ---")
        rows = score_all(args.source)
        print(f"scored {len(rows)} statements")
    if args.step in ("analyze", "all"):
        print("--- analyze ---")
        analyze(rows, args.source)


if __name__ == "__main__":
    main()
