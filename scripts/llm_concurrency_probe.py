#!/usr/bin/env python3
"""
Concurrency diagnostic — measures the TWO numbers that decide the architecture.

It answers, with data instead of argument:

  (1) H1 vs H2 + plateau-K  — a K-sweep against ONE endpoint: aggregate tokens/sec
      as a function of concurrent in-flight requests K ∈ {1,2,4,8,16,32,64}.
        rises then plateaus  -> concurrency-starved (H2); plateau-K = target
                                per-server concurrency to aim for.
        flat from K=1        -> genuinely throughput-bound (H1); only more
                                vLLM replicas help.
      This is a controlled micro-benchmark against a SINGLE server, so it is
      immune to the shard->port distribution bug — run it today.

  (2) threads vs processes  — the client CPU/IO split per call. Threads overlap
      I/O but the GIL serializes pure-Python CPU (eviction-victim selection,
      BM25 retrieval, prompt build). By Amdahl, thread speedup caps at
      ~1/CPU_fraction regardless of thread count. So:
        I/O-dominated  -> threads win (one dataset copy, race-free, no async).
        CPU-dominated  -> threads plateau; you need processes (or a hybrid:
                          threads for the LLM wait + ProcessPool for the CPU).

Run on the box that can reach the endpoint (LUMI compute node / your Ollama host):

    python scripts/llm_concurrency_probe.py --base-url http://localhost:8000/v1 \
        --model local --api-key sk-lumi

Gotchas handled: the httpx connection pool is sized >= max(K) (else extra threads
block at the pool, not the server, and you measure a lie); each timing loop uses
its own isolated objects.
"""
from __future__ import annotations

import argparse
import statistics as st
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
from openai import OpenAI

from forgetit.settings import LLM_BASE_URL, LLM_API_KEY, LLM_MODEL

# A prompt that reliably produces ~max_tokens of output (summarisation-shaped,
# like the compression policy's real calls).
_LOREM = ("The user discussed deployment timelines, Redis ACL rules, JWT expiry, "
          "Socket.io reconnection, and message persistence across several sessions. ") * 12
_PROMPT = (f"Summarize the following into a short paragraph keeping key facts.\n\nTEXT:\n{_LOREM}\n\nSUMMARY:")


# ── (1) K-sweep ────────────────────────────────────────────────────

def _make_client(base_url: str, api_key: str, max_conn: int) -> OpenAI:
    # Size the pool ABOVE the max concurrency we'll test, or threads queue at the
    # client instead of the server and the sweep undercounts concurrency.
    limits = httpx.Limits(max_connections=max_conn, max_keepalive_connections=max_conn)
    return OpenAI(base_url=base_url, api_key=api_key,
                  http_client=httpx.Client(limits=limits, timeout=httpx.Timeout(180.0)))


def _one_call(client: OpenAI, model: str, max_tokens: int) -> tuple[float, int]:
    t0 = time.perf_counter()
    r = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": _PROMPT}],
        max_tokens=max_tokens, temperature=0.0,
    )
    dt = time.perf_counter() - t0
    usage = getattr(r, "usage", None)
    if usage is not None and getattr(usage, "completion_tokens", None):
        toks = usage.completion_tokens
    else:  # endpoints that omit usage (some Ollama builds): approximate
        toks = len((r.choices[0].message.content or "").split())
    return dt, toks


def k_sweep(client, model, ks, max_tokens, reps):
    print(f"\n=== (1) K-sweep — tokens/sec vs concurrent in-flight requests ===")
    print(f"{'K':>4} {'calls':>6} {'wall(s)':>8} {'tok/s':>9} {'lat_p50(s)':>11} {'lat_p95(s)':>11}")
    best = (0, 0.0)
    for K in ks:
        n = K * reps
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=K) as ex:
            res = list(ex.map(lambda _: _one_call(client, model, max_tokens), range(n)))
        wall = time.perf_counter() - t0
        lat = sorted(d for d, _ in res)
        toks = sum(t for _, t in res)
        tps = toks / wall if wall else 0
        p = lambda q: lat[min(len(lat) - 1, int(q * len(lat)))]
        print(f"{K:>4} {n:>6} {wall:>8.1f} {tps:>9.0f} {p(0.50):>11.2f} {p(0.95):>11.2f}")
        if tps > best[1]:
            best = (K, tps)
    print(f"\nplateau ≈ K={best[0]} ({best[1]:.0f} tok/s). If tok/s kept rising you're "
          f"still concurrency-starved at K={ks[-1]} — extend the sweep. If it was flat "
          f"from K=1, you're throughput-bound (need replicas, not concurrency).")


# ── (2) client CPU vs LLM-IO split ─────────────────────────────────

def _bench(fn, n: int) -> float:
    for _ in range(min(3, n)):
        fn()
    t0 = time.perf_counter()
    for _ in range(n):
        fn()
    return (time.perf_counter() - t0) / n


def cpu_io_split(client, model, max_tokens, n_items, item_bytes, can_call_llm):
    from forgetit.core.schema import MemRecord, Query
    from forgetit.core.logging import BenchmarkLogger
    from forgetit.backend.in_memory import InMemoryBackend
    from forgetit.memory_system import RetentionManager
    from forgetit.policies import LFUPolicy
    from forgetit.retrieval import BM25Retriever

    print(f"\n=== (2) client CPU vs LLM-IO per call ({n_items} items × ~{item_bytes}B) ===")

    backend = InMemoryBackend(retriever=BM25Retriever())
    store = RetentionManager(budget_bytes=1 << 62, backend=backend,
                             policy=LFUPolicy(), logger=BenchmarkLogger())
    store.connect()
    pad = "lorem ipsum dolor sit amet consectetur "
    for i in range(n_items):
        txt = f"session {i}: " + pad * (item_bytes // len(pad) + 1)
        store.insert(MemRecord(id=f"s{i}", text=txt[:item_bytes], created_at=1.0 + i))

    q = Query(text="What did the user say about deployment timelines and Redis ACL rules?")

    t_sel = _bench(lambda: LFUPolicy().select_victim(store), 500)
    t_ret = _bench(lambda: backend.search(q, 5), 500)
    retrieved = backend.search(q, 5)
    t_prompt = _bench(lambda: "\n".join(r.text for r in retrieved), 2000)

    print(f"  victim selection (LFU over {n_items}): {t_sel*1e3:8.3f} ms")
    print(f"  retrieval (BM25, k=5):                 {t_ret*1e3:8.3f} ms")
    print(f"  prompt assembly (k=5 context):         {t_prompt*1e3:8.3f} ms")

    if can_call_llm:
        lat = st.median(_one_call(client, model, max_tokens)[0] for _ in range(5))
        print(f"  LLM round-trip (max_tokens={max_tokens}):       {lat*1e3:8.1f} ms")
    else:
        lat = None
        print("  LLM round-trip: endpoint unreachable — re-run on the host that can "
              "reach it, or compute the ratio with your measured latency.")

    if lat:
        for name, cpu in (("compression eviction (select+prompt)", t_sel + t_prompt),
                          ("reader query (retrieve+prompt)",       t_ret + t_prompt)):
            frac = cpu / (cpu + lat)
            ceil = 1.0 / frac if frac > 0 else float("inf")
            print(f"  {name:<38} CPU={cpu*1e3:6.2f}ms  CPU_frac={frac:6.2%}  "
                  f"thread ceiling ≈ {ceil:5.1f}×")
        print("\n  I/O-dominated (CPU_frac small, high ceiling) -> threads win. "
              "CPU-dominated (low ceiling) -> need processes / hybrid.")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base-url", default=LLM_BASE_URL)
    p.add_argument("--api-key", default=LLM_API_KEY or "sk-noauth")
    p.add_argument("--model", default=LLM_MODEL or "local")
    p.add_argument("--ks", default="1,2,4,8,16,32,64", help="comma-separated concurrency levels")
    p.add_argument("--reps", type=int, default=4, help="calls per K = K × reps")
    p.add_argument("--max-tokens", type=int, default=128)
    p.add_argument("--items", type=int, default=50, help="store size for the CPU timing")
    p.add_argument("--item-bytes", type=int, default=10_000)
    p.add_argument("--ksweep-only", action="store_true")
    p.add_argument("--cpu-only", action="store_true")
    args = p.parse_args()

    ks = [int(x) for x in args.ks.split(",") if x.strip()]
    client = _make_client(args.base_url, args.api_key, max(ks) + 8)

    # Probe reachability once (short timeout) so we degrade gracefully off-host.
    can_call = False
    try:
        _make_client(args.base_url, args.api_key, 4).chat.completions.create(
            model=args.model, messages=[{"role": "user", "content": "ping"}],
            max_tokens=1, temperature=0.0, timeout=10.0)
        can_call = True
    except Exception as e:
        print(f"[warn] endpoint {args.base_url} unreachable ({type(e).__name__}): "
              f"K-sweep skipped; CPU timings still run.")

    print(f"endpoint={args.base_url}  model={args.model}  Ks={ks}  reps={args.reps}")

    if can_call and not args.cpu_only:
        k_sweep(client, args.model, ks, args.max_tokens, args.reps)
    if not args.ksweep_only:
        cpu_io_split(client, args.model, args.max_tokens, args.items, args.item_bytes, can_call)


if __name__ == "__main__":
    main()
