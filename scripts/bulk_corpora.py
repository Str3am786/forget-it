"""D5 — bulk the golden corpora to manifest counts (golden_corpora/README.md).

Generates *_bulk.yaml SIBLING files next to the frozen exemplar files (which
are never rewritten — rule 5). Every generated fixture ships verified: false.

Expected values for corpus_e come from an INDEPENDENT mini-simulator of the
documented store semantics (budget loop before admit / incoming exempt per
QUESTIONS.md Q1; ticks advance per insert+touch; victim rules as pinned by the
verified exemplars) — ~60 lines of arithmetic that import nothing from
forgetit, so a pass against the real store is a genuine differential check.
Class-logic guards (e.g. an lru_touch fixture must CHANGE its victim when the
touches are removed) are asserted at generation time.

Deterministic (seed 20260708). Rerun: python scripts/bulk_corpora.py
"""
from __future__ import annotations

import random
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
CORP = REPO / "golden_corpora"
SEED = 20260708

# compression policy constants mirrored from LLMCompressionPolicy defaults —
# asserted in the fixtures' `assumptions` so drift is visible.
MIN_COMPRESS_BYTES = 200
MIN_SAVING_RATIO = 0.5


# ════════════════════════════════════════════════════════════════════
# corpus_e — independent mini-simulator (imports nothing from forgetit)
# ════════════════════════════════════════════════════════════════════


def simulate(policy: str, budget: int, events: list[dict],
             stub_chars: int | None = None, script: dict | None = None) -> dict:
    res: dict[str, dict] = {}      # id -> {bytes,last,freq,t_insert,created,comp}
    tick = 0
    evictions: list[str] = []
    compressions: list[str] = []

    def victim(cands, rule):
        keys = {
            "lru": lambda i: (res[i]["last"], res[i]["created"], i),
            "lfu": lambda i: (res[i]["freq"], res[i]["last"], res[i]["created"], i),
            "fifo": lambda i: (res[i]["t_insert"], res[i]["created"], i),
            "keep_smallest": lambda i: (-res[i]["bytes"], res[i]["created"], i),
            "evict_smallest": lambda i: (res[i]["bytes"], res[i]["created"], i),
        }
        return min(cands, key=keys[rule])

    def used():
        return sum(r["bytes"] for r in res.values())

    def pressure_step(incoming: int):
        while used() + incoming > budget and res:
            if policy == "compression":
                unc = [i for i in res if not res[i]["comp"]]
                if unc:
                    v = victim(unc, "lfu")
                    ob = res[v]["bytes"]
                    saving = 1 - (stub_chars / ob)
                    if ob < MIN_COMPRESS_BYTES or saving < MIN_SAVING_RATIO:
                        evictions.append(v); del res[v]
                    else:
                        compressions.append(v)
                        res[v]["bytes"] = stub_chars
                        res[v]["comp"] = True
                else:
                    v = victim(res, "lfu")
                    evictions.append(v); del res[v]
            else:
                v = victim(res, policy)
                evictions.append(v); del res[v]

    for step, ev in enumerate(events, 1):
        if ev["op"] == "insert":
            pressure_step(ev["size"])
            tick += 1
            res[ev["item"]] = {"bytes": ev["size"], "last": tick, "freq": 0,
                               "t_insert": tick, "created": step, "comp": False}
        elif ev["op"] == "touch":
            tick += 1
            res[ev["item"]]["freq"] += 1
            res[ev["item"]]["last"] = tick
        elif ev["op"] == "step":
            pass
        if policy == "scripted_eviction":
            for rid in (script or {}).get(step, []):
                assert rid in res, f"script names ghost {rid}"
                evictions.append(rid); del res[rid]

    return {"evictions": evictions, "compressions": compressions,
            "resident": sorted(res),
            "resident_bytes": {i: res[i]["bytes"] for i in sorted(res)}}


def gen_corpus_e(rng: random.Random) -> list[dict]:
    out: list[dict] = []
    nid = 9

    def emit(cls, inp, expected, rationale, assumptions):
        nonlocal nid
        out.append({"id": f"e{nid:03d}", "class": cls, "verified": False,
                    "assumptions": assumptions, "input": inp,
                    "expected": expected, "rationale": rationale})
        nid += 1

    names = "ABCDEFGH"

    # ── lru_touch ×8: touches must CHANGE the victim (e001/e002 pairing) ──
    made = 0
    while made < 8:
        n = rng.choice([3, 4, 5])
        size = rng.choice([30, 40, 50])
        budget = size * (n - 1) + size // 2          # last insert forces 1 eviction
        events = [{"op": "insert", "item": names[i], "size": size} for i in range(n - 1)]
        # touch 1–2 of the earliest items before the final insert
        touched = rng.sample(range(n - 2), k=min(rng.choice([1, 2]), n - 2)) if n > 2 else [0]
        for t in sorted(touched):
            events.append({"op": "touch", "item": names[t]})
        events.append({"op": "insert", "item": names[n - 1], "size": size})
        exp = simulate("lru", budget, events)
        blind = simulate("lru", budget, [e for e in events if e["op"] != "touch"])
        if exp["evictions"] == blind["evictions"]:
            continue                                  # touch didn't matter → not this class
        emit("lru_touch", {"policy": "lru", "budget": budget, "events": events}, exp,
             f"LRU with touches on {sorted(names[t] for t in touched)}: recency order at the "
             f"final insert makes {exp['evictions']} the victim(s); without the touches the "
             f"victim would be {blind['evictions']} — the touch bookkeeping is the class logic.",
             ["touch updates recency", "eviction loop runs inside insert only"])
        made += 1

    # ── lru_multi_evict_loop ×5: one stream, ≥2 evictions ──
    made = 0
    while made < 5:
        big = rng.choice([70, 80, 90])
        small = rng.choice([20, 30])
        budget = big + small + rng.choice([0, 10])
        events = [{"op": "insert", "item": "A", "size": big},
                  {"op": "insert", "item": "B", "size": big},
                  {"op": "insert", "item": "C", "size": small},
                  {"op": "insert", "item": "D", "size": big}]
        exp = simulate("lru", budget, events)
        if len(exp["evictions"]) < 2:
            continue
        emit("lru_multi_evict_loop", {"policy": "lru", "budget": budget, "events": events}, exp,
             f"Budget {budget} against sizes [{big},{big},{small},{big}]: the loop must fire "
             f"repeatedly (evictions {exp['evictions']}), re-reading recency state between "
             f"evictions — loop termination and state-refresh are the class logic.",
             ["loop evicts repeatedly until total <= budget"])
        made += 1

    # ── lfu_tiebreak ×3: all freq 0 → insertion order ──
    for _ in range(3):
        n = rng.choice([3, 4])
        size = rng.choice([35, 40])
        budget = size * (n - 1) + size // 2
        events = [{"op": "insert", "item": names[i], "size": size} for i in range(n)]
        exp = simulate("lfu", budget, events)
        emit("lfu_tiebreak", {"policy": "lfu", "budget": budget, "events": events}, exp,
             "All frequencies 0 — the tie-break IS the policy: insertion order "
             f"(oldest insert tick loses → {exp['evictions']}).",
             ["freq counts touches only (inserts do not count)",
              "tie-break = insertion order (last_access seeded at insert tick)"])

    # ── lfu_freq ×3: touches make LFU diverge from LRU ──
    made = 0
    while made < 3:
        size = rng.choice([30, 40])
        budget = 2 * size + size // 2
        # A gets MANY touches early; B touched once LATER (B is most-recent by
        # LRU but low-freq by LFU → the two policies pick different victims).
        events = [{"op": "insert", "item": "A", "size": size},
                  {"op": "insert", "item": "B", "size": size}]
        events += [{"op": "touch", "item": "A"}] * rng.choice([2, 3])
        events += [{"op": "touch", "item": "B"}]
        events += [{"op": "insert", "item": "C", "size": size}]
        exp = simulate("lfu", budget, events)
        lru_exp = simulate("lru", budget, events)
        if exp["evictions"] == lru_exp["evictions"]:
            continue
        emit("lfu_freq", {"policy": "lfu", "budget": budget, "events": events}, exp,
             f"Freqs at C's insert: A high, B=1; LFU evicts {exp['evictions']} while LRU "
             f"would evict {lru_exp['evictions']} — frequency (not recency) is the class "
             "logic. Incoming item C is EXEMPT (budget loop precedes its admit; Q1).",
             ["freq counts touches only",
              "incoming item EXEMPT from eviction (implementation semantics — QUESTIONS.md Q1)"])
        made += 1

    # ── fifo ×5: touches ignored ──
    made = 0
    while made < 5:
        n = rng.choice([4, 5])
        size = rng.choice([30, 40])
        budget = size * (n - 2) + size // 2
        events = []
        for i in range(n - 2):
            events.append({"op": "insert", "item": names[i], "size": size})
        events.append({"op": "touch", "item": names[0]})       # must be ignored
        events.append({"op": "insert", "item": names[n - 2], "size": size})
        events.append({"op": "insert", "item": names[n - 1], "size": size})
        exp = simulate("fifo", budget, events)
        lru_exp = simulate("lru", budget, events)
        if exp["evictions"] == lru_exp["evictions"]:
            continue                                  # touch must discriminate fifo from lru
        emit("fifo", {"policy": "fifo", "budget": budget, "events": events}, exp,
             f"FIFO evicts in insertion order {exp['evictions']} even though {names[0]} was "
             f"touched (LRU would evict {lru_exp['evictions']}) — touch-ignoring is the class "
             "logic, keyed on the store's insert ordinal, not data timestamps.",
             ["FIFO first-class", "eviction order == insertion order, touches ignored"])
        made += 1

    # ── size_policy_arith ×7 (5 keep_smallest / 2 evict_smallest) ──
    for i in range(7):
        pol = "keep_smallest" if i < 5 else "evict_smallest"
        sizes = rng.sample([10, 20, 35, 50, 60, 75], k=3)
        # incoming (last) must never be the would-be victim, so eligibility
        # semantics cannot matter (e007's robustness pattern)
        incoming = sorted(sizes)[1]
        first_two = [s for s in sizes if s != incoming][:2]
        budget = sum(first_two) + incoming - 5
        events = [{"op": "insert", "item": "A", "size": first_two[0]},
                  {"op": "insert", "item": "B", "size": first_two[1]},
                  {"op": "insert", "item": "C", "size": incoming}]
        exp = simulate(pol, budget, events)
        emit("size_policy_arith", {"policy": pol, "budget": budget, "events": events}, exp,
             f"{pol}: victim at pressure is the {'largest' if pol == 'keep_smallest' else 'smallest'} "
             f"resident of sizes {first_two} → {exp['evictions']}; incoming C ({incoming}B) is "
             "mid-sized so eligibility semantics cannot change the answer.",
             [f"{pol} victim rule", "fixture is incoming-eligibility-robust by construction"])

    # ── scripted_eviction ×5 ──
    for i in range(5):
        n_items = rng.choice([3, 4])
        evict_step = n_items + 1                       # the step op after all inserts
        victims = rng.sample([names[j] for j in range(n_items)], k=rng.choice([1, 2]))
        script = {evict_step: victims}
        events = [{"op": "insert", "item": names[j], "size": 40} for j in range(n_items)]
        events.append({"op": "step"})
        exp = simulate("scripted_eviction", 10 ** 6, events, script=script)
        emit("scripted_eviction",
             {"policy": "scripted_eviction", "script": {"evict_at_step": script},
              "budget": 10 ** 6, "events": events}, exp,
             f"Fiat eviction of {victims} at step {evict_step}; budget can never trigger. "
             "The eviction trace must equal the script exactly.",
             ["scripted_eviction policy exists", "budget never triggers"])

    # ── compression_trigger ×6 (deterministic stub summariser) ──
    comp_cases = [
        # (sizes, budget, stub, touches, rationale-tag)
        ([300, 300, 300], 650, 30, [], "basic compress-instead-of-evict"),
        ([300, 300, 300], 650, 30, ["A"], "LFU selector: touched A survives, B compressed first"),
        ([150, 300, 250], 400, 30, [], "sub-floor item (150B < 200B) is EVICTED, next is compressed"),
        ([300, 300, 300], 650, 200, [], "reject: saving 1-200/300 < 0.5 → evict fallback"),
        ([260, 260, 900], 700, 30, [], "multiple compressions in one activation"),
        ([240, 240, 60, 60, 60], 220, 30, [], "all-compressed then still over → evict phase"),
    ]
    for sizes, budget, stub, touches, tag in comp_cases:
        events = []
        for j, s in enumerate(sizes[:-1]):
            events.append({"op": "insert", "item": names[j], "size": s})
        for t in touches:
            events.append({"op": "touch", "item": t})
        events.append({"op": "insert", "item": names[len(sizes) - 1], "size": sizes[-1]})
        exp = simulate("compression", budget, events, stub_chars=stub)
        emit("compression_trigger",
             {"policy": "compression", "stub_summary_chars": stub,
              "budget": budget, "events": events}, exp,
             f"{tag}: stub always returns {stub}B; floor {MIN_COMPRESS_BYTES}B and saving "
             f"ratio {MIN_SAVING_RATIO} are the policy defaults. Expected "
             f"compressions={exp['compressions']} evictions={exp['evictions']} — "
             "the S4 accounting path (compress-to-pack, evict fallback), not summary quality.",
             [f"min_compress_bytes={MIN_COMPRESS_BYTES}", f"min_saving_ratio={MIN_SAVING_RATIO}",
              "compress selector = LFU over uncompressed residents",
              "stub summariser (plumbing corpus — no LLM)"])

    assert len(out) == 42, len(out)
    return out


def main() -> None:
    rng = random.Random(SEED)
    e_bulk = gen_corpus_e(rng)
    path = CORP / "corpus_e" / "e_fixtures_bulk.yaml"
    header = ("# corpus_e BULK fixtures — generated by scripts/bulk_corpora.py "
              f"(seed {SEED}).\n# Exemplars live in e_fixtures.yaml (frozen). "
              "Every fixture here is verified: false until the author checks it.\n")
    path.write_text(header + yaml.safe_dump(e_bulk, sort_keys=False, width=100))
    print(f"wrote {path} ({len(e_bulk)} fixtures)")

    from bulk_corpora_data import gen_corpus_r, gen_corpus_a, gen_verdicts
    r_bulk = gen_corpus_r(random.Random(SEED + 1))
    path = CORP / "corpus_r" / "r_fixtures_bulk.yaml"
    path.write_text("# corpus_r BULK fixtures — generated by scripts/bulk_corpora.py.\n"
                    + yaml.safe_dump(r_bulk, sort_keys=False, width=100))
    print(f"wrote {path} ({len(r_bulk)} fixtures)")

    a_bulk = gen_corpus_a(random.Random(SEED + 2))
    path = CORP / "corpus_a_reader" / "reader_fixtures_bulk.yaml"
    path.write_text("# corpus_a_reader BULK fixtures — generated by scripts/bulk_corpora.py.\n"
                    + yaml.safe_dump(a_bulk, sort_keys=False, width=100))
    print(f"wrote {path} ({len(a_bulk)} fixtures)")

    j_bulk = gen_verdicts(random.Random(SEED + 3))
    path = CORP / "corpus_a_judge" / "verdict_triplets_bulk.yaml"
    path.write_text("# verdict_triplets BULK fixtures — generated by scripts/bulk_corpora.py.\n"
                    + yaml.safe_dump(j_bulk, sort_keys=False, width=100))
    print(f"wrote {path} ({len(j_bulk)} fixtures)")


if __name__ == "__main__":
    main()
