#!/usr/bin/env python3
"""
Preflight for one served reader. Runs in under a minute and gates the sweep.

Three checks, each guarding a failure that is invisible in aggregate accuracy
and fatal to a cross family comparison.

  1. TEMPLATE. Does this model's chat template render the reader prompt with the
     structure the prompt assumes? Gemma 3 has no system role and folds system
     content into the first user turn, so a prompt written against a system plus
     user layout arrives at Gemma as something structurally different. Left
     unchecked, that difference is attributed to the family in the results.

  2. FORMAT COMPLIANCE. Tier one grading is exact match and containment. A model
     that wraps its answer in a preamble is penalised for style rather than for
     knowledge, and preamble rates correlate with size in exactly the direction
     that would fake a capacity effect. Measured on short span items with known
     answers.

  3. TOKENISER PARITY. The evidence budget is denominated in tokens and
     tokenisers differ across families, so an identical budget delivers a
     different amount of text to each reader. Reports characters per token on
     real evidence text against the pinned reader's ratio. A divergence above
     the threshold means the budget is a covariate, not a constant, and the
     analysis must say so.

Exit code 0 on pass, 1 on failure when --strict is given. Without --strict it
always exits 0 and writes the verdict to the JSON, which is the right default
for the first pass over a new reader: you want the numbers, not a dead job.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Thresholds. These are pre registration, not tuning knobs: fix them before the
# first reader runs and do not move them to make a rung pass.
# ---------------------------------------------------------------------------
MIN_FORMAT_COMPLIANCE = 0.90     # fraction of answers with no preamble/suffix
MAX_CPT_DIVERGENCE = 0.15        # |chars-per-token ratio - pinned| / pinned
PINNED_READER = "qwen2.5-7b-instruct"

# Short span probes with unambiguous single token-ish answers. Deliberately
# trivial: this measures obedience to the output contract, not capability, so a
# 1.5B failing here is a format finding and not a knowledge finding.
FORMAT_PROBES = [
    ("Evidence:\n[1] Maria moved to Lisbon in 2019.\n\nQuestion: Which city did Maria move to?", "Lisbon"),
    ("Evidence:\n[1] The package weighed 4 kilograms.\n\nQuestion: How heavy was the package?", "4"),
    ("Evidence:\n[1] Tom's appointment is on 14 February.\n\nQuestion: On what date is Tom's appointment?", "14 February"),
    ("Evidence:\n[1] The blue folder is on the top shelf.\n\nQuestion: Where is the blue folder?", "top shelf"),
    ("Evidence:\n[1] Priya adopted a cat named Basil.\n\nQuestion: What is the cat's name?", "Basil"),
    ("Evidence:\n[1] The meeting was moved to Thursday.\n\nQuestion: Which day is the meeting?", "Thursday"),
    ("Evidence:\n[1] Alex paid 30 euros for the ticket.\n\nQuestion: How much was the ticket?", "30"),
    ("Evidence:\n[1] The library closes at 8 pm on weekdays.\n\nQuestion: When does the library close on weekdays?", "8"),
]

# Abstention contract: an unanswerable item must produce a refusal. A reader
# that never declines makes the abstention metric degenerate, which matters more
# for this thesis than for most, since unanswerable categories carry a headline.
ABSTAIN_PROBES = [
    "Evidence:\n[1] The shop sells bread and cheese.\n\nQuestion: What is the shop owner's surname?",
    "Evidence:\n[1] It rained on Tuesday.\n\nQuestion: What was the temperature on Tuesday?",
    "Evidence:\n[1] Sam went for a run.\n\nQuestion: How far did Sam run?",
]

SYSTEM = (
    "Answer the question using only the evidence provided. "
    "Reply with the answer alone and nothing else. "
    "If the evidence does not contain the answer, reply exactly: I don't know."
)

# Representative evidence text for the tokeniser parity check. Conversational
# register, since that is what the itemisers actually produce.
PARITY_TEXT = (
    "On Tuesday afternoon Priya mentioned that she had finally booked the flights "
    "to Porto for the last week of September, and that her brother would be joining "
    "her for the first three days before heading on to Madrid. She said the hotel "
    "near the river had been fully booked, so she settled on a smaller place two "
    "streets back, which was cheaper anyway. She also asked whether anyone had a "
    "spare adaptor she could borrow, because hers had stopped working during the "
    "trip to Rome in June."
) * 4

PREAMBLE = re.compile(
    r"^\s*(based on|according to|the answer is|from the evidence|sure[,!]|"
    r"here is|i can see|it (appears|seems)|as (per|stated))",
    re.I,
)
ABSTAIN = re.compile(r"\b(i don'?t know|not (specified|stated|mentioned|provided)|"
                     r"cannot be determined|no information)\b", re.I)


def post(url: str, payload: dict, api_key: str, timeout: int = 120) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def chat(base: str, key: str, model: str, user: str, system: str | None = SYSTEM) -> dict:
    msgs = ([{"role": "system", "content": system}] if system else []) + [
        {"role": "user", "content": user}
    ]
    return post(
        f"{base.rstrip('/')}/chat/completions",
        {"model": model, "messages": msgs, "temperature": 0, "max_tokens": 256},
        key,
    )


def count_tokens(base: str, key: str, model: str, text: str) -> int | None:
    """Use the server's tokeniser so the count is the one that actually meters
    the evidence budget, rather than a local approximation."""
    try:
        out = post(f"{base.rstrip('/')}/../tokenize".replace("/v1/../", "/"),
                   {"model": model, "prompt": text}, key, timeout=60)
        return out.get("count") or len(out.get("tokens", [])) or None
    except Exception:
        return None


# ---------------------------------------------------------------------------

def check_template(base: str, key: str, model: str, model_path: str) -> dict:
    """Does the model accept a system role at all, and does the rendered prompt
    keep system and user separate?"""
    result: dict = {"name": "template"}

    # Read the template from the staged checkpoint, so the finding survives even
    # if the server rejects the request.
    tmpl = None
    for candidate in ("tokenizer_config.json", "chat_template.json"):
        p = Path(model_path) / candidate
        if p.is_file():
            try:
                blob = json.loads(p.read_text())
                tmpl = blob.get("chat_template") or tmpl
            except Exception:
                pass
    result["has_chat_template"] = tmpl is not None
    # Templates that fold system into the first user turn are recognisable: they
    # mention the system role but never emit a distinct system header.
    if tmpl:
        result["template_mentions_system"] = "system" in tmpl
        result["template_raises_on_system"] = "raise_exception" in tmpl and "system" in tmpl

    try:
        r = chat(base, key, model, "Reply with the single word: ready.")
        result["system_role_accepted"] = True
        result["sample"] = r["choices"][0]["message"]["content"].strip()[:120]
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:400]
        result["system_role_accepted"] = False
        result["error"] = body
        # This is the Gemma case. It is survivable, but the prompt must be
        # merged into the user turn for this reader and that fact recorded,
        # because it changes the rendered prompt hash.
        result["remedy"] = (
            "This reader rejects a separate system message. Set the reader to "
            "merge system content into the first user turn for this family, and "
            "record the merged prompt sha in provenance: the rendered prompt is "
            "no longer identical across the axis and the methods must say so."
        )
        return result

    # Merged rendering as a control: if merged and separate give different
    # answers on a trivial item, the prompt structure is doing work.
    merged = chat(base, key, model, SYSTEM + "\n\n" + FORMAT_PROBES[0][0], system=None)
    separate = chat(base, key, model, FORMAT_PROBES[0][0])
    result["merged_vs_separate_agree"] = (
        merged["choices"][0]["message"]["content"].strip().lower()
        == separate["choices"][0]["message"]["content"].strip().lower()
    )
    return result


def check_format(base: str, key: str, model: str) -> dict:
    clean, rows = 0, []
    for prompt, expected in FORMAT_PROBES:
        try:
            r = chat(base, key, model, prompt)
        except Exception as exc:
            rows.append({"expected": expected, "error": str(exc)[:200]})
            continue
        msg = r["choices"][0]["message"]["content"]
        fin = r["choices"][0].get("finish_reason")
        ans = msg.strip()
        is_clean = (
            not PREAMBLE.search(ans)
            and "\n" not in ans.strip()
            and len(ans) <= max(40, len(expected) * 6)
        )
        clean += is_clean
        rows.append({
            "expected": expected,
            "answer": ans[:160],
            "contains_expected": expected.lower() in ans.lower(),
            "clean": is_clean,
            "finish_reason": fin,
        })

    abstained, arows = 0, []
    for prompt in ABSTAIN_PROBES:
        try:
            ans = chat(base, key, model, prompt)["choices"][0]["message"]["content"].strip()
        except Exception as exc:
            arows.append({"error": str(exc)[:200]})
            continue
        did = bool(ABSTAIN.search(ans))
        abstained += did
        arows.append({"answer": ans[:160], "abstained": did})

    n_f, n_a = len(FORMAT_PROBES), len(ABSTAIN_PROBES)
    return {
        "name": "format",
        "format_compliance": round(clean / n_f, 3),
        "content_accuracy": round(sum(r.get("contains_expected", False) for r in rows) / n_f, 3),
        "abstention_rate_on_unanswerable": round(abstained / n_a, 3),
        "detail": rows,
        "abstain_detail": arows,
    }


def check_tokeniser(base: str, key: str, model: str, reader_id: str, ref_cpt: float | None) -> dict:
    n = count_tokens(base, key, model, PARITY_TEXT)
    out: dict = {"name": "tokeniser", "chars": len(PARITY_TEXT), "tokens": n}
    if not n:
        out["note"] = (
            "The /tokenize endpoint was unavailable, so parity could not be "
            "measured here. Fall back to the delivered token counts already "
            "recorded per row and compare them across readers in analysis."
        )
        return out
    cpt = len(PARITY_TEXT) / n
    out["chars_per_token"] = round(cpt, 3)
    if reader_id == PINNED_READER or ref_cpt is None:
        out["role"] = "reference"
        return out
    div = abs(cpt - ref_cpt) / ref_cpt
    out.update(reference_chars_per_token=ref_cpt, divergence=round(div, 3),
               within_threshold=div <= MAX_CPT_DIVERGENCE)
    if div > MAX_CPT_DIVERGENCE:
        out["remedy"] = (
            "This reader's tokeniser packs evidence at a materially different "
            "density from the pinned reader, so the token denominated budget "
            "delivers a different amount of text. Report delivered evidence in "
            "characters as the primary figure and treat the budget as a "
            "covariate for this family."
        )
    return out


def bench_throughput(base: str, key: str, model: str, n: int, concurrency: int) -> dict:
    """Measure rows per second at a representative prompt length.

    The replay grid guard needs a real number rather than an assumed one, and
    throughput varies by a large factor across the roster: a 1.5B on eight
    replicas and a 14B on eight replicas are not the same machine. Measuring it
    here costs about thirty seconds and turns 'dies at the wall with a partial
    CSV' into 'refuses to start'.
    """
    import concurrent.futures as cf
    import time

    # Roughly the shape of a real reader call: five items against a 16,000 token
    # budget, capped generation. Prompt length dominates prefill, which is where
    # the time goes.
    evidence = "\n\n".join(
        f"[{i + 1}] {PARITY_TEXT}" for i in range(5)
    )
    prompt = f"Evidence:\n{evidence}\n\nQuestion: Where did Priya book flights to?"

    def one(_):
        t0 = time.perf_counter()
        try:
            chat(base, key, model, prompt)
            return time.perf_counter() - t0
        except Exception:
            return None

    t_start = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        lat = [x for x in ex.map(one, range(n)) if x is not None]
    wall = time.perf_counter() - t_start

    if not lat:
        return {"throughput_rps": None, "note": "every benchmark call failed"}
    return {
        "throughput_rps": round(len(lat) / wall, 3),
        "n_calls": len(lat),
        "n_failed": n - len(lat),
        "concurrency": concurrency,
        "latency_p50_s": round(statistics.median(lat), 3),
        "latency_max_s": round(max(lat), 3),
        "prompt_chars": len(prompt),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--api-key", default="sk-lumi")
    ap.add_argument("--model", required=True, help="served-model-name")
    ap.add_argument("--reader-id", required=True)
    ap.add_argument("--model-path", required=True)
    ap.add_argument("--out", required=True)
    # Tokeniser density is measured once at staging and carried in
    # readers.lock.json. The previous design read the pinned reader's
    # preflight.json, which could not work: array tasks run concurrently, so
    # there was no guarantee the pinned reader had written its file first.
    ap.add_argument("--reference-cpt", type=float, default=None,
                    help="pinned reader's chars per token, from readers.lock.json")
    ap.add_argument("--measured-cpt", type=float, default=None,
                    help="this reader's chars per token, from readers.lock.json")
    ap.add_argument("--bench", type=int, default=0,
                    help="measure throughput with N representative calls; feeds the grid guard")
    ap.add_argument("--bench-concurrency", type=int, default=8)
    ap.add_argument("--strict", action="store_true",
                    help="exit non-zero on failure; use once thresholds are agreed")
    args = ap.parse_args()

    ref_cpt = args.reference_cpt

    checks = {
        "template": check_template(args.base_url, args.api_key, args.model, args.model_path),
        "format": check_format(args.base_url, args.api_key, args.model),
        "tokeniser": check_tokeniser(args.base_url, args.api_key, args.model, args.reader_id, ref_cpt),
    }

    # Prefer the staged measurement over the live one: it is the tokeniser that
    # actually metered the evidence, measured on identical text for every reader.
    if args.measured_cpt:
        checks["tokeniser"]["chars_per_token_staged"] = args.measured_cpt
        if ref_cpt:
            div = abs(args.measured_cpt - ref_cpt) / ref_cpt
            checks["tokeniser"].update(
                reference_chars_per_token=ref_cpt,
                divergence=round(div, 3),
                within_threshold=div <= MAX_CPT_DIVERGENCE,
                source="readers.lock.json",
            )

    bench = {}
    if args.bench:
        bench = bench_throughput(
            args.base_url, args.api_key, args.model, args.bench, args.bench_concurrency
        )
        checks["throughput"] = bench

    failures = []
    if checks["format"]["format_compliance"] < MIN_FORMAT_COMPLIANCE:
        failures.append(
            f"format compliance {checks['format']['format_compliance']:.2f} "
            f"< {MIN_FORMAT_COMPLIANCE}: tier one grading would be scoring style"
        )
    if checks["format"]["abstention_rate_on_unanswerable"] == 0.0:
        failures.append(
            "this reader never declined on an unanswerable item: its abstention "
            "numbers are a property of the reader and must be reported as such"
        )
    if checks["tokeniser"].get("within_threshold") is False:
        failures.append("tokeniser density diverges from the pinned reader beyond threshold")
    if checks["template"].get("system_role_accepted") is False:
        failures.append("system role rejected: prompt must be merged for this family")

    verdict = {
        "reader_id": args.reader_id,
        "served_model": args.model,
        "pass": not failures,
        "failures": failures,
        # Top level so the launcher's grid guard can read it without walking
        # into checks{}. None means the guard is skipped rather than wrong.
        "throughput_rps": bench.get("throughput_rps"),
        "thresholds": {
            "min_format_compliance": MIN_FORMAT_COMPLIANCE,
            "max_cpt_divergence": MAX_CPT_DIVERGENCE,
        },
        "checks": checks,
    }
    Path(args.out).write_text(json.dumps(verdict, indent=2) + "\n")

    print(f"\n[preflight] {args.reader_id}")
    print(f"  format compliance          {checks['format']['format_compliance']:.2f}")
    print(f"  content accuracy (trivial) {checks['format']['content_accuracy']:.2f}")
    print(f"  abstains on unanswerable   {checks['format']['abstention_rate_on_unanswerable']:.2f}")
    print(f"  chars per token            {checks['tokeniser'].get('chars_per_token', 'n/a')}")
    print(f"  system role accepted       {checks['template'].get('system_role_accepted')}")
    if failures:
        print("  VERDICT: attention needed")
        for f in failures:
            print(f"    - {f}")
    else:
        print("  VERDICT: pass")
    print(f"  written to {args.out}\n")

    return 1 if (failures and args.strict) else 0


if __name__ == "__main__":
    raise SystemExit(main())
