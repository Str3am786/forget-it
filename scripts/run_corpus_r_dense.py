"""Run the dense-retriever corpus_r fixtures (paraphrase_needle_dense).

Needs the embedding endpoint; the deterministic classes run in
tests/corpora/test_corpus_r.py. Needle bar: 100% (VERIFICATION.md §7) —
r002 is dense-only BY DESIGN (BM25 is expected to fail it, so it is not run
under bm25 anywhere).

Usage:  EMBED_BASE_URL=http://192.168.0.74:11434/v1 python scripts/run_corpus_r_dense.py
Output: report/corpus_r_dense.json
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from forgetit.core.schema import MemRecord, Query
from forgetit.embeddings import get_embedder
from forgetit.embeddings.ingest import attach_embeddings
from forgetit.retrieval import EmbeddingRetriever

from tests.corpora.harness import load_fixtures


def main() -> int:
    embedder = get_embedder()
    retriever = EmbeddingRetriever(embedder=embedder)
    fixtures = [f for f in load_fixtures("corpus_r")
                if f["class"] == "paraphrase_needle_dense"]
    rows, n_pass = [], 0
    for fx in fixtures:
        inp = fx["input"]
        items = {}
        for i, (k, t) in enumerate(inp["docs"].items(), 1):
            rec = MemRecord(id=k, text=t, created_at=float(i), bytes=len(t))
            items[k] = rec
        attach_embeddings(list(items.values()), embedder)
        results = retriever.topk(items, Query(id="q", text=inp["query"],
                                              timestamp=1.0, meta={}), int(inp["k"]))
        ids = [r.id for r in results]
        ok, detail = True, []
        for doc_id, want in fx["expected"]["rank_of"].items():
            rank = ids.index(doc_id) + 1 if doc_id in ids else None
            bound = int(want["lte"]) if isinstance(want, dict) else int(want)
            hit = rank is not None and rank <= bound
            ok &= hit
            detail.append(f"{doc_id} rank={rank} (need <= {bound})")
        n_pass += ok
        rows.append({"id": fx["id"], "pass": ok, "detail": detail,
                     "top3": ids[:3], "query": inp["query"]})
        print(f"  {fx['id']} {'PASS' if ok else 'FAIL'} {detail}")
    out = {"embedder": getattr(embedder, "model", "?"), "n": len(rows),
           "passed": n_pass, "rows": rows}
    (REPO / "report" / "corpus_r_dense.json").write_text(json.dumps(out, indent=1))
    print(f"{n_pass}/{len(rows)} → report/corpus_r_dense.json")
    return 0 if n_pass == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
