from __future__ import annotations

import argparse
import json
import re
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Tuple


_WORDY_DATE = re.compile(r"^\s*(\d{4})/(\d{2})/(\d{2})\s*\([A-Za-z]{3}\)\s*(\d{2}):(\d{2})\s*$")


def parse_longmemeval_date(x: Any) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    if not isinstance(x, str):
        raise ValueError(f"Bad date type: {type(x)}")
    m = _WORDY_DATE.match(x)
    if not m:
        raise ValueError(f"Unrecognized date format: {x!r}")
    y, mo, d, hh, mm = map(int, m.groups())
    return dt.datetime(y, mo, d, hh, mm).timestamp()


def flatten_session(session_turns: List[Dict[str, Any]]) -> str:
    parts = []
    for turn in session_turns:
        role = turn.get("role", "unknown")
        content = turn.get("content", "")
        parts.append(f"{role}: {content}")
    return "\n".join(parts)


def pctl(sorted_vals: List[int], q: float) -> int:
    if not sorted_vals:
        return 0
    idx = int(round((len(sorted_vals) - 1) * q))
    return sorted_vals[max(0, min(idx, len(sorted_vals) - 1))]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    args = ap.parse_args()

    data_path = Path(args.data)
    examples = json.loads(data_path.read_text(encoding="utf-8"))

    session_bytes: List[int] = []
    example_totals: List[int] = []
    example_sessions: List[int] = []

    for ex in examples:
        sids = ex["haystack_session_ids"]
        dates = ex["haystack_dates"]
        sess = ex["haystack_sessions"]

        triples = list(zip(sids, dates, sess))
        triples.sort(key=lambda t: parse_longmemeval_date(t[1]))

        total = 0
        for _, _, turns in triples:
            text = flatten_session(turns)
            b = len(text.encode("utf-8"))
            session_bytes.append(b)
            total += b

        example_totals.append(total)
        example_sessions.append(len(triples))

    session_bytes.sort()
    example_totals.sort()
    example_sessions.sort()

    def show(name: str, vals: List[int]) -> None:
        print(f"\n{name}: n={len(vals)}")
        for q, label in [(0.50, "p50"), (0.75, "p75"), (0.90, "p90"), (0.95, "p95"), (0.99, "p99")]:
            v = pctl(vals, q)
            print(f"  {label}: {v:,} bytes")

    show("Session bytes (one MemRecord)", session_bytes)
    show("Example total bytes (store all sessions)", example_totals)
    show("Sessions per example", example_sessions)


if __name__ == "__main__":
    main()