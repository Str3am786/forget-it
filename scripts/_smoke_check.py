import json, os
from collections import defaultdict, Counter

ROOT = "artifacts/reader_smoke/longmemeval_turn_pair"
delivered = defaultdict(dict)

for rid in sorted(os.listdir(ROOT)):
    d = os.path.join(ROOT, rid)
    if not os.path.isdir(d):
        continue
    files = [f for f in os.listdir(d) if f.endswith(".jsonl")]
    if not files:
        print(f"{rid:<28} NO ROWS"); continue
    rows = []
    for f in files:
        with open(os.path.join(d, f)) as fh:
            rows += [json.loads(l) for l in fh if l.strip()]

    dup = [k for k, c in Counter((r["config"], r["query_id"], r["arm"])
                                 for r in rows).items() if c > 1]
    models = {r.get("model", "") for r in rows}
    acc = {}
    for arm in ("parametric", "empty", "retrieved", "gold"):
        ok = [r for r in rows if r["arm"] == arm and not r["errored"]]
        acc[arm] = sum(r["correct"] for r in ok) / len(ok) if ok else float("nan")
    err = sum(r["errored"] for r in rows) / len(rows)

    print(f"\n{rid}")
    print(f"  rows {len(rows):<6} dup {len(dup):<4} err {err:.3f}  model={models}")
    print("  " + "  ".join(f"{a}={acc[a]:.3f}" for a in acc))
    print(f"  gold - retrieved = {acc['gold'] - acc['retrieved']:+.3f}"
          + ("   VIOLATION: retrieved beats gold" if acc["retrieved"] > acc["gold"] else ""))

    for r in rows:
        if r["arm"] == "retrieved":
            delivered[(r["config"], r["query_id"])][rid] = r["n_delivered"]

    p = os.path.join(ROOT, f"preflight.{rid}.json")
    if os.path.isfile(p):
        v = json.load(open(p))
        c = v["checks"]["format"]
        print(f"  preflight compliance {c['format_compliance']}  "
              f"abstain {c['abstention_rate_on_unanswerable']}  "
              f"rps {v.get('throughput_rps')}")
        for f in v["failures"]:
            print(f"    - {f}")

bad = [k for k, v in delivered.items() if len(set(v.values())) > 1]
print(f"\nn_delivered identical across readers: {len(delivered) - len(bad)}"
      f"/{len(delivered)}  mismatches={len(bad)}")
