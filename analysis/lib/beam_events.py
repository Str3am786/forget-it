"""Workload-characterization loader: turn BEAM event logs into item tables.

Source: artifacts/benchmark_full/beam/... (the current run). Item sizes, gold
membership, reference counts, and the access trace are properties of the
ITEMISER, not the policy — so we read them from the no-eviction control
(budget_frac=1.0), where every item is inserted and nothing is evicted, giving
the complete, undistorted stream + access trace.

Per itemiser we return:
  items    — DataFrame(example_idx, item_id, bytes, insert_pos, is_gold,
             gold_links, access_count)
  accesses — ordered access trace per example: insert (first reference) + each
             retrieval hit (re-reference). Used for reuse distance + Mattson MRC.
  queries  — DataFrame(ex_idx, query_id, gold_ids, n_gold)

"gold_links" = how many scored queries reference an item as gold (value-relevant,
retriever-independent — the reference count that gates LFU/LRU). "access_count" =
retrieval hits across all probes+queries at budget 1.0 (retriever-dependent; what
LFU would actually see).
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from beam_load import BEAM_ROOT

# Representative no-eviction config. Sizes + gold are retriever/policy-independent;
# the access trace is retriever-dependent, so we fix bm25 and say so. lru@100pct
# evicts nothing, so the stream is complete.
_REPR_POLICY = "lru"
_REPR_RETRIEVER = "bm25"
_REPR_BUDGET = "budget_100pct"


def _events_path(itemiser: str) -> Path:
    return (BEAM_ROOT / itemiser / "retention" / _REPR_POLICY / _REPR_RETRIEVER
            / _REPR_BUDGET / "PlainTextReader" / "events.jsonl")


def load_itemiser(itemiser: str):
    """Parse the no-eviction event log for one itemiser. Examples are segmented on
    synthetic ``clear`` rows. Returns (items_df, access_trace, queries_df)."""
    path = _events_path(itemiser)
    if not path.exists():
        raise FileNotFoundError(path)

    ex_idx = -1
    item_bytes: dict[tuple, int] = {}
    item_insert: dict[tuple, int] = {}
    gold_links: dict[tuple, int] = {}
    access_cnt: dict[tuple, int] = {}
    access_trace: dict[int, list[str]] = {}
    q_rows: list[dict] = []
    pos = 0

    def new_example():
        nonlocal ex_idx, pos
        ex_idx += 1
        pos = 0
        access_trace[ex_idx] = []

    new_example()
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            name = ev.get("name")
            if name == "clear":
                new_example()
            elif name == "insert":
                iid = ev["id"]
                key = (ex_idx, iid)
                if key not in item_bytes:
                    item_bytes[key] = int(ev.get("bytes", 0))
                    item_insert[key] = pos
                    gold_links.setdefault(key, 0)
                    access_cnt.setdefault(key, 0)
                    pos += 1
                access_trace[ex_idx].append(iid)
            elif name == "retrieve":
                for hid in ev.get("hits", []):
                    key = (ex_idx, hid)
                    access_cnt[key] = access_cnt.get(key, 0) + 1
                    access_trace[ex_idx].append(hid)
            elif name == "gold":
                gids = ev.get("gold_ids", [])
                for gid in gids:
                    key = (ex_idx, gid)
                    gold_links[key] = gold_links.get(key, 0) + 1
                q_rows.append({"ex_idx": ex_idx, "query_id": ev.get("query_id", ""),
                               "gold_ids": list(gids), "n_gold": len(gids)})

    rows = []
    for (eid, iid), b in item_bytes.items():
        rows.append({
            "example_idx": eid, "item_id": iid, "bytes": b,
            "insert_pos": item_insert[(eid, iid)],
            "gold_links": gold_links.get((eid, iid), 0),
            "access_count": access_cnt.get((eid, iid), 0),
            "is_gold": gold_links.get((eid, iid), 0) > 0,
        })
    return pd.DataFrame(rows), access_trace, pd.DataFrame(q_rows)


def reuse_and_creation_distances(items: pd.DataFrame, access_trace: dict[int, list[str]]):
    """creation_to_need (gold items, single-reference reuse distance = n_inserts −
    insert_pos) and stack_distance (distinct items between successive references —
    classical LRU stack distance, defined only for repeated references)."""
    n_inserts = items.groupby("example_idx")["insert_pos"].max() + 1
    gold = items[items.is_gold].copy()
    gold["creation_to_need"] = gold.apply(
        lambda r: int(n_inserts[r.example_idx]) - int(r.insert_pos), axis=1)

    stack = []
    for eid, trace in access_trace.items():
        last_seen: dict[str, int] = {}
        for i, iid in enumerate(trace):
            if iid in last_seen:
                stack.append(len(set(trace[last_seen[iid] + 1:i])))
            last_seen[iid] = i
    return gold["creation_to_need"].to_numpy(), stack


if __name__ == "__main__":
    for it in ("beam_turn", "beam_turn_pair", "beam_batch"):
        items, trace, queries = load_itemiser(it)
        print(f"\n=== {it} ===")
        print(f"  examples={items.example_idx.nunique()} items={len(items)} "
              f"gold_items={items.is_gold.sum()} queries={len(queries)}")
        print(f"  bytes: p50={int(items.bytes.median())} CV={items.bytes.std()/items.bytes.mean():.2f}")
        print(f"  access_count: max={items.access_count.max()} mean={items.access_count.mean():.2f} "
              f">1x={(items.access_count>1).mean():.3f}")
        print(f"  gold_links: max={items.gold_links.max()} "
              f"mean_over_gold={items[items.is_gold].gold_links.mean():.2f} "
              f">1x_over_gold={(items[items.is_gold].gold_links>1).mean():.3f}")
