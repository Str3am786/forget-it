"""Instrument-parity attestation (sprint-2 fence). Run BEFORE any measured run
counts, on EVERY serving stack — the error budget is per-stack.

Everything comes from env vars (``.env`` or exported; exports win):
    LLM_BASE_URL, LLM_API_KEY        chat endpoint (OpenAI-compatible)
    READER_MODEL (or LLM_MODEL)      the model tag under attestation
    EMBED_BASE_URL, EMBED_MODEL      embeddings endpoint (Q2: same box is fine)

Checks (all must pass; exit 1 otherwise):
  1. liveness    — chat completion answers; embeddings endpoint returns a vector
  2. identity    — the reader tag appears in the endpoint's /v1/models list
  3. canary      — a ~12k-token prompt with HEAD and TAIL sentinels must echo
                   BOTH (front- and back-truncation detection in one probe);
                   the server-reported prompt token count is recorded as the
                   stack's measured context evidence
  4. baseline    — a fixed 6-fixture corpus subset through the D4 reader
                   config: a001 (verbatim) and a002 (absent→abstain) GATE the
                   attestation (plumbing: the model answers and abstention
                   detection works end-to-end); the position triplet
                   a006–a008 is RECORDED for the scorecard, not gated here
                   (quality bars belong to evaluate_reader_candidate.py).

On success writes ``report2/attestation/<fingerprint>.json`` and
``report2/attestation/latest.json``; the candidate scorecard REFUSES to run
without a fresh attestation whose fingerprint matches the live env.

Usage:  python scripts/attest_environment.py [--target-tokens 12000] [--max-age-note]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OUT_DIR = REPO / "report2" / "attestation"

sys.path.insert(0, str(REPO))                      # for tests.corpora reuse
sys.path.insert(0, str(REPO / "scripts"))

from openai import OpenAI

from forgetit.settings import (
    LLM_API_KEY, LLM_BASE_URL, EMBED_BASE_URL, EMBED_MODEL, EMBED_API_KEY,
)
from context_canary import build_prompt, HEAD_SENTINEL, TAIL_SENTINEL

BASELINE_FIXTURES = ["a001", "a002", "a005", "a006", "a007", "a008"]
GATED_FIXTURES = {"a001", "a002"}


def reader_model() -> str:
    m = os.environ.get("READER_MODEL") or os.environ.get("LLM_MODEL")
    if not m:
        from forgetit.settings import LLM_MODEL
        m = LLM_MODEL
    if not m:
        sys.exit("attestation: set READER_MODEL (or LLM_MODEL)")
    return m


def stack_fingerprint(model: str) -> str:
    raw = json.dumps([LLM_BASE_URL, model, EMBED_BASE_URL, EMBED_MODEL])
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def check_liveness(client: OpenAI, model: str) -> dict:
    r = client.chat.completions.create(
        model=model, messages=[{"role": "user", "content": "Reply with the word: ready"}],
        temperature=0.0, max_tokens=8)
    chat_ok = "ready" in (r.choices[0].message.content or "").lower()
    emb_client = OpenAI(base_url=EMBED_BASE_URL, api_key=EMBED_API_KEY or "sk-noauth")
    e = emb_client.embeddings.create(model=EMBED_MODEL, input="liveness probe")
    emb_ok = len(e.data[0].embedding) > 0
    return {"chat_ok": chat_ok, "embeddings_ok": emb_ok,
            "embedding_dim": len(e.data[0].embedding), "ok": chat_ok and emb_ok}


def check_identity(client: OpenAI, model: str) -> dict:
    served = [m.id for m in client.models.list().data]
    return {"served_count": len(served), "reader_tag_served": model in served,
            "ok": model in served}


def check_canary(client: OpenAI, model: str, target_tokens: int) -> dict:
    prompt = build_prompt(target_tokens)
    r = client.chat.completions.create(
        model=model, messages=[{"role": "user", "content": prompt}],
        temperature=0.0, max_tokens=80)
    out = (r.choices[0].message.content or "").strip()
    usage = getattr(r, "usage", None)
    head, tail = HEAD_SENTINEL in out, TAIL_SENTINEL in out
    return {"target_tokens": target_tokens,
            "measured_prompt_tokens": getattr(usage, "prompt_tokens", None),
            "head_echoed": head, "tail_echoed": tail,
            "diagnosis": ("ok" if head and tail else
                          "FRONT truncation (num_ctx too small)" if tail else
                          "BACK truncation or refusal" if head else "no sentinels"),
            "ok": head and tail}


def check_baseline(model: str) -> dict:
    """Fixed corpus subset via the corpus_a runner machinery (D4 config)."""
    import yaml
    from run_corpus_a import FIXTURE_FILES, build_evidence, check_expected
    from forgetit.core.schema import Question
    from forgetit.llm.openai_llm import OpenAiLLM
    from forgetit.llm.readers import PlainTextReader

    fixtures = []
    for path in FIXTURE_FILES:
        if path.exists():
            fixtures.extend(yaml.safe_load(path.read_text()))
    by_id = {f["id"]: f for f in fixtures}
    llm = OpenAiLLM(base_url=LLM_BASE_URL, api_key=LLM_API_KEY, model=model)
    reader = PlainTextReader(llm, token_budget=4000, prompt_version="v2",
                             abstention_match="exact", clean_mode="full")
    rows = {}
    for fid in BASELINE_FIXTURES:
        fx = by_id[fid]
        records = build_evidence(fx)
        res = reader.answer(Question(id=fid, text=fx["input"]["question"],
                                     timestamp=0.0, meta={}), records, max_tokens=256)
        ok, details = check_expected(fx, res.answer, res.abstained)
        rows[fid] = {"pass": ok, "details": details, "answer": res.answer[:120]}
    gated_ok = all(rows[f]["pass"] for f in GATED_FIXTURES)
    return {"rows": rows, "gated": sorted(GATED_FIXTURES),
            "position_triplet_recorded": {f: rows[f]["pass"]
                                          for f in ("a006", "a007", "a008")},
            "ok": gated_ok}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-tokens", type=int, default=12_000)
    args = ap.parse_args()

    model = reader_model()
    client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY or "sk-noauth")
    record = {"attested_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
              "llm_base_url": LLM_BASE_URL, "reader_model": model,
              "embed_base_url": EMBED_BASE_URL, "embed_model": EMBED_MODEL,
              "fingerprint": stack_fingerprint(model), "sections": {}}

    failures = []
    for name, fn in [("liveness", lambda: check_liveness(client, model)),
                     ("identity", lambda: check_identity(client, model)),
                     ("canary", lambda: check_canary(client, model, args.target_tokens)),
                     ("baseline", lambda: check_baseline(model))]:
        try:
            section = fn()
        except Exception as exc:
            section = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        record["sections"][name] = section
        status = "PASS" if section.get("ok") else "FAIL"
        print(f"[{status}] {name}: "
              f"{json.dumps({k: v for k, v in section.items() if k != 'rows'}, default=str)[:220]}")
        if not section.get("ok"):
            failures.append(name)

    record["ok"] = not failures
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"{record['fingerprint']}.json"
    path.write_text(json.dumps(record, indent=1))
    (OUT_DIR / "latest.json").write_text(json.dumps(record, indent=1))
    print(f"\nattestation {'PASS' if record['ok'] else 'FAIL: ' + ', '.join(failures)}"
          f" → {path} (+ latest.json)")
    return 0 if record["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
