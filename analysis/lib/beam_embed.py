"""Re-embed BEAM items with the RUN's embedder (qwen3-embedding:0.6b) and cache.

Validated: recomputed query_coverage reproduces the run's stored values to ~0.006
mean abs error, so these vectors are effectively the ones the run's semantic
policies saw. Vectors are cached (content-addressed sqlite) so this runs once.

Endpoint config (the run's): base_url :11434/v1, model qwen3-embedding:0.6b,
1024-dim, L2-normalized, 8000-char input cap (matches the run).

Saves per itemiser an npz: item_id, conv_idx, is_gold, vectors (N×1024).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE.parent / "src"))
from forgetit.embeddings.openai_embedder import OpenAiEmbedder  # noqa: E402
from forgetit.embeddings.cache import EmbeddingCache, CachingEmbedder  # noqa: E402
from forgetit.benchmark.workloads.beam import (  # noqa: E402
    BeamWorkload, BeamTurnItemiser, BeamTurnPairItemiser, BeamBatchItemiser)

EMBED_URL = "http://192.168.0.74:11434/v1"
EMBED_MODEL = "qwen3-embedding:0.6b"
DATA = HERE.parent / "data" / "beam_500K.jsonl"
CACHE_DB = HERE / "tables" / "_recomputed_embeddings.sqlite"   # my cache, NOT the run's
_ITEMISERS = {"beam_turn": BeamTurnItemiser, "beam_turn_pair": BeamTurnPairItemiser,
              "beam_batch": BeamBatchItemiser}


def _embedder():
    inner = OpenAiEmbedder(base_url=EMBED_URL, api_key="sk-noauth", model=EMBED_MODEL,
                           normalize=True, max_input_chars=8000, batch_size=64,
                           max_connections=32)
    return CachingEmbedder(inner, EmbeddingCache(CACHE_DB))


def _embed_concurrent(emb, texts, batch=32, workers=4):
    """Concurrent, INCREMENTAL, OBSERVABLE embedding.

    Fixes the black-box run: each batch is written to the cache the moment it
    returns (durable + resumable), and progress is printed as done/total with a
    live rate. Concurrency is kept to ~Ollama's parallel-slot count (default 4) so
    we saturate the GPU without head-of-line-blocking fresh requests. Cache de-dups,
    so a re-run resumes for free — only misses hit the network.
    """
    import sys as _sys
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from forgetit.embeddings.cache import _key
    inner, cache = emb.inner, emb.cache

    keys = [_key(emb.name, t) for t in texts]
    hits = cache.get_many(list(set(keys)))
    miss_idx = [i for i, k in enumerate(keys) if k not in hits]
    n_miss = len(miss_idx)
    print(f"[embed] {len(texts)} items | cache hits {len(texts)-n_miss} | to embed {n_miss} "
          f"| batch={batch} workers={workers}", file=_sys.stderr, flush=True)
    if n_miss:
        chunks = [miss_idx[i:i + batch] for i in range(0, n_miss, batch)]

        def _do(chunk_idxs):
            vecs = inner.embed([texts[i] for i in chunk_idxs])
            cache.put_many([(keys[i], v) for i, v in zip(chunk_idxs, vecs)])  # durable now
            return chunk_idxs, vecs

        done = 0; t0 = _time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = [pool.submit(_do, c) for c in chunks]
            for fut in as_completed(futs):
                cidx, vecs = fut.result()
                for i, v in zip(cidx, vecs):
                    hits[keys[i]] = v
                done += len(cidx)
                rate = done / max(_time.perf_counter() - t0, 1e-6)
                eta = (n_miss - done) / max(rate, 1e-6)
                print(f"[embed]  {done}/{n_miss} ({100*done/n_miss:4.1f}%)  "
                      f"{rate:.0f} items/s  ETA {eta:4.0f}s", file=_sys.stderr, flush=True)
    return [hits[k] for k in keys]


def npz_path(itemiser: str) -> Path:
    return HERE / "tables" / f"_emb_{itemiser}.npz"


def build(itemiser: str = "beam_turn_pair", force: bool = False) -> dict:
    p = npz_path(itemiser)
    if p.exists() and not force:
        d = np.load(p, allow_pickle=True)
        return {k: d[k] for k in d.files}

    wl = BeamWorkload(DATA, itemiser=_ITEMISERS[itemiser](),
                      probe_every=0, clear_between_examples=True)
    emb = _embedder()
    item_ids, conv_idx, is_gold, texts = [], [], [], []
    for ci, (conv_id, evs) in enumerate(wl.iter_examples()):
        gold = set()
        for e in evs:
            if e.type == "query" and e.gold_item_ids:
                gold.update(e.gold_item_ids)
        for e in evs:
            if e.type == "insert" and e.item is not None:
                item_ids.append(e.item.id); conv_idx.append(ci)
                is_gold.append(e.item.id in gold); texts.append(e.item.text or "")
    vecs = np.asarray(_embed_concurrent(emb, texts), dtype=np.float32)
    out = {"item_id": np.array(item_ids, dtype=object), "conv_idx": np.array(conv_idx),
           "is_gold": np.array(is_gold), "vectors": vecs}
    np.savez(p, **out)
    return out


def load_partial(itemiser: str = "beam_turn_pair") -> dict:
    """Load whatever is ALREADY cached (no network) for COMPLETE conversations only.

    Lets the redundancy metric run on a growing subset while the full embed is still
    in flight: a conversation is included iff every one of its items is in the cache
    (so within-conversation redundancy is exact). Returns the same keys as build()
    plus n_convs_complete / n_convs_total.
    """
    from forgetit.embeddings.cache import _key, EmbeddingCache
    wl = BeamWorkload(DATA, itemiser=_ITEMISERS[itemiser](),
                      probe_every=0, clear_between_examples=True)
    inner = OpenAiEmbedder(base_url=EMBED_URL, api_key="sk-noauth", model=EMBED_MODEL,
                           normalize=True, max_input_chars=8000)
    cache = EmbeddingCache(CACHE_DB)
    per_conv = []
    for ci, (conv_id, evs) in enumerate(wl.iter_examples()):
        gold = {g for e in evs if e.type == "query" and e.gold_item_ids for g in e.gold_item_ids}
        items = [(e.item.id, e.item.text or "") for e in evs if e.type == "insert" and e.item]
        per_conv.append((ci, gold, items))
    keys_by_item = {(ci, iid): _key(inner.name, txt) for ci, _g, items in per_conv for iid, txt in items}
    got = cache.get_many(list(set(keys_by_item.values())))
    ids, conv_idx, is_gold, vecs = [], [], [], []
    n_complete = 0
    for ci, gold, items in per_conv:
        if all(keys_by_item[(ci, iid)] in got for iid, _t in items) and items:
            n_complete += 1
            for iid, _t in items:
                ids.append(iid); conv_idx.append(ci); is_gold.append(iid in gold)
                vecs.append(got[keys_by_item[(ci, iid)]])
    return {"item_id": np.array(ids, dtype=object), "conv_idx": np.array(conv_idx),
            "is_gold": np.array(is_gold), "vectors": np.asarray(vecs, dtype=np.float32),
            "n_convs_complete": n_complete, "n_convs_total": len(per_conv)}


if __name__ == "__main__":
    it = sys.argv[1] if len(sys.argv) > 1 else "beam_turn_pair"
    d = build(it, force="--force" in sys.argv)
    v = d["vectors"]
    print(f"{it}: {v.shape[0]} items × {v.shape[1]}d | gold={int(d['is_gold'].sum())} "
          f"| convs={len(set(d['conv_idx'].tolist()))} | norm~{np.linalg.norm(v[0]):.3f}")
