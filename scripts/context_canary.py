"""Context-window canary (VERIFICATION.md §0.5 / S6, D0 attestation).

Builds a prompt with a HEAD sentinel at the very top and a TAIL sentinel at the
bottom, separated by ~TARGET_TOKENS of deterministic filler, and asks the model
to echo both codes. Ollama front-truncates when the prompt exceeds the served
num_ctx, so a lost HEAD sentinel means the tag's real context is smaller than
the prompt — regardless of what the tag NAME claims. Exit 0 iff both sentinels
are echoed; prints the server-reported prompt token count as the measured size.

Usage:
    python scripts/context_canary.py [--model TAG] [--target-tokens N]

Defaults target the 16k serving tag with a ~12k-token prompt (the D0 check:
num_ctx >= 16384 asserted behaviourally, not by tag name).
"""
from __future__ import annotations

import argparse
import random
import sys

from openai import OpenAI

from forgetit.settings import LLM_BASE_URL, LLM_API_KEY

HEAD_SENTINEL = "kappa-7391-obsidian"
TAIL_SENTINEL = "zephyr-2846-marigold"

# Neutral vocabulary for filler — no numbers, no sentinel fragments.
_WORDS = (
    "harbor meadow lantern gravel timber orchard saddle copper mantle willow "
    "ledger anchor barley canvas drizzle ember flagon gutter hollow ingot "
    "jetty kestrel lumber marble nectar oxbow pallet quarry rafter shingle "
    "tackle upland vellum wharf yonder zenith bramble crofter dapple eaves"
).split()


def build_prompt(target_tokens: int, seed: int = 42) -> str:
    rng = random.Random(seed)
    # ~1.3 tokens/word is a decent prior for these tokenizers; the server's
    # usage.prompt_tokens is printed afterwards as the measured truth.
    n_words = int(target_tokens / 1.3)
    filler_words = [rng.choice(_WORDS) for _ in range(n_words)]
    lines = []
    for i in range(0, len(filler_words), 12):
        lines.append(" ".join(filler_words[i:i + 12]) + ".")
    filler = "\n".join(lines)
    return (
        f"You will answer a question at the end.\n"
        f"HEAD CODE: {HEAD_SENTINEL}\n"
        f"\n"
        f"Filler notes (ignore their content):\n"
        f"{filler}\n"
        f"\n"
        f"TAIL CODE: {TAIL_SENTINEL}\n"
        f"\n"
        f"Question: output the HEAD CODE and the TAIL CODE exactly, nothing else.\n"
        f"Answer:"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="qwen2.5:7b-instruct-q5km-16k")
    ap.add_argument("--target-tokens", type=int, default=12_000)
    args = ap.parse_args()

    prompt = build_prompt(args.target_tokens)
    client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY or "sk-noauth")
    resp = client.chat.completions.create(
        model=args.model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        max_tokens=80,
    )
    out = (resp.choices[0].message.content or "").strip()
    usage = getattr(resp, "usage", None)
    prompt_tokens = getattr(usage, "prompt_tokens", None)

    head_ok = HEAD_SENTINEL in out
    tail_ok = TAIL_SENTINEL in out
    print(f"model={args.model}")
    print(f"prompt_chars={len(prompt)} server_prompt_tokens={prompt_tokens}")
    print(f"head_echoed={head_ok} tail_echoed={tail_ok}")
    print(f"raw_output={out!r}")
    if head_ok and tail_ok:
        print("CANARY PASS: both sentinels echoed — served context covers the prompt.")
        return 0
    if tail_ok and not head_ok:
        print("CANARY FAIL: tail-only echo — FRONT truncation; served num_ctx is "
              "smaller than the prompt (the 4096 failure mode).")
    else:
        print("CANARY FAIL: sentinel(s) missing from output.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
