"""Offline LLM judge over run artifacts — ONE system, two modes (E1 + E2).

E1 — recoverability bracket: for each (query, gold item) in
``gold_answer_snapshot.jsonl``, judge whether the gold answer is recoverable
from the item's RESIDENT text (possibly compressed; "" if evicted). Blind to
policy and budget. Bounds the truth between string-containment (harsh on
paraphrase) and id-survival (blind to compression loss).

E2 — answer re-grade: for each scored query in ``results.csv``, judge the
recorded reader answer against the reference (per-question-type rubrics), or
against the gold items' content when the reference is empty. The instrument
for the degenerate deterministic grader; the deterministic grade remains the
reproducible headline.

Design commitments (§7.5 of the plan):
  - judge model family MUST differ from the reader family (checked, hard error);
  - temperature 0; verdict-first output; strict parser + one re-ask retry;
  - disk cache keyed on (model, prompt_version, prompt) → re-runs are free and
    interrupted runs resume;
  - honeypots interleaved at a fixed rate as an online health metric — the
    batch ABORTS if the running pass rate drops below threshold;
  - blindness is structural (see judge_prompts) and tested;
  - every output row stamps (judge_model, prompt_version).

CLI:
    python -m forgetit.benchmark.judge --mode e1 --runs 'artifacts/**/budget_*/PlainTextReader' [--limit 20]
    python -m forgetit.benchmark.judge --mode e2 --runs ... --second-judge <model>
    python -m forgetit.benchmark.judge --mode e1 --runs ... --export-validation sample.csv
    python -m forgetit.benchmark.judge --score-validation labeled.csv --validation-key sample.csv.key.csv

Outputs land NEXT TO each config's results.csv as judge_e1.jsonl /
judge_e2.jsonl plus judge_<mode>_summary.json.

JUDGE_MODEL="mock" selects a deterministic offline stub (containment
heuristics) for plumbing tests and dry-runs on hosts without a served judge.
Its rows are stamped judge_model="mock" and must never enter a figure.
"""
from __future__ import annotations

import argparse
import csv
import glob as globlib
import hashlib
import json
import logging
import os
import random
import re
import sys
from collections import defaultdict
from pathlib import Path

from forgetit.benchmark.grading import answer_in_text, answer_recall, contains_match
from forgetit.benchmark.judge_prompts import (
    PROMPT_VERSION,
    build_e1_prompt,
    build_e2_goldtext_prompt,
    build_e2_prompt,
)

log = logging.getLogger(__name__)

E1_ALLOWED = ("YES", "NO")
E2_ALLOWED = ("CORRECT", "INCORRECT")
_JUDGE_MAX_TOKENS = 128
_SEED = 42


# ── model family separation ─────────────────────────────────────────

_FAMILIES = ("qwen", "llama", "mixtral", "mistral", "gemma", "phi", "gpt",
             "claude", "deepseek", "olmo", "command", "granite", "nemotron",
             "yi", "glm", "vicuna", "falcon", "o1", "o3")


def model_family(model_name: str) -> str:
    """Coarse family tag for the family-separation check ('unknown' if none)."""
    s = (model_name or "").lower()
    for fam in _FAMILIES:
        if fam in s:
            return "mistral" if fam == "mixtral" else fam
    return "unknown"


def check_family_separation(judge_model: str, reader_model: str) -> None:
    jf, rf = model_family(judge_model), model_family(reader_model)
    if judge_model == "mock":
        return
    if jf == "unknown" or rf == "unknown":
        log.warning("family check inconclusive (judge=%s → %s, reader=%s → %s) "
                    "— verify manually that the families differ.",
                    judge_model, jf, reader_model, rf)
        return
    if jf == rf:
        raise SystemExit(
            f"Judge model {judge_model!r} and reader model {reader_model!r} "
            f"are the same family ({jf}). §7.5: the judge family must differ "
            f"from the reader family — pick a different JUDGE_MODEL.")


# ── clients ─────────────────────────────────────────────────────────

def build_judge_client(model: str, base_url: str | None, api_key: str | None):
    if model == "mock":
        return MockJudge()
    from forgetit.llm.openai_llm import OpenAiLLM
    from forgetit.settings import LLM_API_KEY, LLM_BASE_URL
    return OpenAiLLM(
        base_url=base_url or os.getenv("JUDGE_BASE_URL", LLM_BASE_URL),
        api_key=api_key or os.getenv("JUDGE_API_KEY", LLM_API_KEY or "sk-noauth"),
        model=model,
    )


class MockJudge:
    """Deterministic offline stub: containment heuristics over the prompt's own
    sections. For plumbing tests and dry-runs ONLY — stamped 'mock' everywhere."""
    model = "mock"

    @staticmethod
    def _section(prompt: str, header: str, end: str) -> str:
        try:
            after = prompt.split(header, 1)[1]
            return after.split(end, 1)[0].strip()
        except IndexError:
            return ""

    def generate(self, prompt: str, **kw) -> str:
        if "Stored memory text:" in prompt:                       # E1
            gold = self._section(prompt, "Correct answer:\n", "\n\nStored memory text:")
            text = self._section(prompt, "---\n", "\n---")
            verdict = "YES" if (gold and answer_in_text(gold, text)) else "NO"
        elif "Conversation excerpts" in prompt:                   # E2 gold-text
            text = self._section(prompt, "---\n", "\n---")
            ans = self._section(prompt, "Answer to grade:\n", "\n\nGrading rule:")
            verdict = "CORRECT" if answer_recall(ans, text) > 0.5 else "INCORRECT"
        else:                                                     # E2 reference
            ref = self._section(prompt, "Reference answer:\n", "\n\nAnswer to grade:")
            ans = self._section(prompt, "Answer to grade:\n", "\n\nGrading rule:")
            verdict = "CORRECT" if (ref and contains_match(ans, ref)) else "INCORRECT"
        return f"VERDICT: {verdict}\nmock heuristic."


# ── verdict parsing ─────────────────────────────────────────────────

_VERDICT_RE = re.compile(r"VERDICT\s*[:\-]?\s*\**\s*([A-Z]+)", re.IGNORECASE)


def parse_verdict(raw: str, allowed: tuple[str, ...]) -> str | None:
    if not raw:
        return None
    m = _VERDICT_RE.search(raw[:400])
    if m and m.group(1).upper() in allowed:
        return m.group(1).upper()
    # Fallback: a bare allowed token on the first non-empty line.
    for line in raw.strip().splitlines()[:2]:
        tok = line.strip().strip('."\'*').upper()
        if tok in allowed:
            return tok
    return None


# ── disk cache ──────────────────────────────────────────────────────

class JudgeCache:
    """Append-only JSONL keyed by sha256(model, prompt_version, prompt).
    Re-runs are free; interrupted runs resume from what's on disk."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._mem: dict[str, dict] = {}
        if self.path.exists():
            with self.path.open(encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        self._mem[rec["key"]] = rec
                    except (json.JSONDecodeError, KeyError):
                        continue        # torn tail line from an interrupted run
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a", encoding="utf-8")

    @staticmethod
    def key(model: str, prompt: str) -> str:
        h = hashlib.sha256()
        h.update(f"{model}\x1f{PROMPT_VERSION}\x1f{prompt}".encode("utf-8"))
        return h.hexdigest()

    def get(self, key: str) -> dict | None:
        return self._mem.get(key)

    def put(self, key: str, record: dict) -> None:
        record = {"key": key, **record}
        self._mem[key] = record
        self._file.write(json.dumps(record) + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()


def judge_one(client, prompt: str, allowed: tuple[str, ...],
              cache: JudgeCache) -> tuple[str | None, bool, bool]:
    """Returns (verdict, parse_error, cache_hit). One re-ask on unparseable."""
    key = cache.key(client.model, prompt)
    hit = cache.get(key)
    if hit is not None:
        return hit.get("verdict"), bool(hit.get("parse_error")), True

    raw = client.generate(prompt, max_tokens=_JUDGE_MAX_TOKENS, temperature=0.0)
    verdict = parse_verdict(raw, allowed)
    if verdict is None:
        retry_prompt = (prompt + "\n\nYour previous reply could not be parsed. "
                        f"Reply with ONLY the verdict line: VERDICT: "
                        f"{' or VERDICT: '.join(allowed)}")
        raw = client.generate(retry_prompt, max_tokens=32, temperature=0.0)
        verdict = parse_verdict(raw, allowed)

    parse_error = verdict is None
    cache.put(key, {"verdict": verdict, "parse_error": parse_error,
                    "raw": (raw or "")[:300]})
    return verdict, parse_error, False


# ── honeypot health metric ──────────────────────────────────────────

class HoneypotMonitor:
    """Tracks the running pass rate of interleaved known-answer probes and
    aborts the batch when the judge stops being trustworthy mid-run."""

    def __init__(self, threshold: float = 0.8, min_count: int = 10):
        self.threshold = threshold
        self.min_count = min_count
        self.passed = 0
        self.total = 0

    def record(self, ok: bool) -> None:
        self.total += 1
        self.passed += int(ok)
        if self.total >= self.min_count and self.pass_rate() < self.threshold:
            raise SystemExit(
                f"HONEYPOT FAILURE: judge pass rate {self.pass_rate():.2f} "
                f"< {self.threshold} after {self.total} honeypots — aborting "
                f"the batch. Verdicts written so far are cached; fix the "
                f"judge/prompt and re-run.")

    def pass_rate(self) -> float:
        return self.passed / self.total if self.total else float("nan")

    def as_dict(self) -> dict:
        return {"honeypots": self.total, "honeypot_passed": self.passed,
                "honeypot_pass_rate": round(self.pass_rate(), 4) if self.total else None,
                "threshold": self.threshold}


# ── run-artifact loading ────────────────────────────────────────────

def find_config_dirs(patterns: list[str], need_snapshot: bool) -> list[Path]:
    dirs: set[Path] = set()
    for pat in patterns:
        for p in globlib.glob(pat, recursive=True):
            path = Path(p)
            if path.is_file():
                path = path.parent
            if (path / "results.csv").exists():
                dirs.add(path)
    out = []
    for d in sorted(dirs):
        if need_snapshot and not (d / "gold_answer_snapshot.jsonl").exists():
            log.warning("skipping %s — no gold_answer_snapshot.jsonl "
                        "(re-run with --event-log to capture it)", d)
            continue
        out.append(d)
    return out


def load_results_rows(config_dir: Path) -> list[dict]:
    with (config_dir / "results.csv").open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_snapshot_rows(config_dir: Path) -> list[dict]:
    rows = []
    with (config_dir / "gold_answer_snapshot.jsonl").open(encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    return rows


_META_COLS = ("policy", "retriever", "budget_bytes", "budget_frac", "itemiser",
              "dataset", "probe_touch_mode", "reader_token_budget", "store")


def _meta_from(result_row: dict) -> dict:
    """Config metadata for OUTPUT rows only — never enters a prompt."""
    return {k: result_row.get(k, "") for k in _META_COLS}


# ── E1 mode ─────────────────────────────────────────────────────────

def run_e1(config_dir: Path, client, cache: JudgeCache, *,
           limit: int | None, honeypot_rate: float, monitor: HoneypotMonitor,
           rng: random.Random) -> list[dict]:
    results = load_results_rows(config_dir)
    by_qid = {r["query_id"]: r for r in results}
    snap = load_snapshot_rows(config_dir)

    out: list[dict] = []
    llm_calls = 0
    pot_every = max(1, int(round(1.0 / honeypot_rate))) if honeypot_rate > 0 else 0
    pot_flip = True         # alternate YES-pot / NO-pot

    for row in snap:
        if limit is not None and len(out) >= limit:
            break
        rrow = by_qid.get(row["query_id"])
        question = (rrow or {}).get("question_text", "")
        meta = _meta_from(rrow) if rrow else {k: "" for k in _META_COLS}
        gold_answer = (row.get("answer") or "").strip()
        resident = (row.get("resident_text") or "").strip()

        base = {
            "example_id": row.get("example_id"), "query_id": row["query_id"],
            "gold_id": row.get("gold_id"), "question_type": row.get("question_type"),
            "was_evicted": bool(row.get("was_evicted")),
            "was_compressed": bool(row.get("was_compressed")),
            "is_honeypot": False, "honeypot_expected": None,
            "judge_model": client.model, "prompt_version": PROMPT_VERSION,
            **meta,
        }

        if not gold_answer:
            out.append({**base, "verdict": None, "rule": "no_reference",
                        "parse_error": False, "cache_hit": False})
            continue
        if not resident:
            # Evicted (or empty) — recoverability is NO by construction, free.
            out.append({**base, "verdict": "NO", "rule": "evicted_empty",
                        "parse_error": False, "cache_hit": False})
            continue

        # Interleave a honeypot before every pot_every-th real LLM call.
        if pot_every and llm_calls and llm_calls % pot_every == 0:
            if pot_flip:
                pot_text = f"user: {question}\nassistant: {gold_answer}"
                expected = "YES"
            else:
                donor = rng.choice(snap)
                pot_text = (donor.get("original_text") or "unrelated text")
                expected = "NO" if donor.get("example_id") != row.get("example_id") else None
            if expected:
                v, perr, chit = judge_one(
                    client, build_e1_prompt(question, gold_answer, pot_text),
                    E1_ALLOWED, cache)
                monitor.record(v == expected)
                out.append({**base, "verdict": v, "rule": "llm",
                            "is_honeypot": True, "honeypot_expected": expected,
                            "parse_error": perr, "cache_hit": chit})
            pot_flip = not pot_flip

        v, perr, chit = judge_one(
            client, build_e1_prompt(question, gold_answer, resident),
            E1_ALLOWED, cache)
        llm_calls += 1
        out.append({**base, "verdict": v, "rule": "llm",
                    "parse_error": perr, "cache_hit": chit})
    return out


def summarize_e1(rows: list[dict], monitor: HoneypotMonitor) -> dict:
    real = [r for r in rows if not r["is_honeypot"]]
    judged = [r for r in real if r["verdict"] in E1_ALLOWED]
    llm_rows = [r for r in judged if r["rule"] == "llm"]
    # Self-falsification: verdict must track id-survival on deletion-only stores.
    agree = [int((r["verdict"] == "YES") == (not r["was_evicted"])) for r in judged]
    agree_llm = [int((r["verdict"] == "YES") == (not r["was_evicted"])) for r in llm_rows]
    deletion_only = not any(r["was_compressed"] for r in real)
    return {
        "mode": "e1", "prompt_version": PROMPT_VERSION,
        "judge_model": rows[0]["judge_model"] if rows else None,
        "n_rows": len(real),
        "n_judged": len(judged),
        "n_llm": len(llm_rows),
        "n_rule_evicted": sum(1 for r in real if r["rule"] == "evicted_empty"),
        "n_no_reference": sum(1 for r in real if r["rule"] == "no_reference"),
        "n_parse_errors": sum(1 for r in real if r["parse_error"]),
        "recoverable_rate": (sum(1 for r in judged if r["verdict"] == "YES")
                             / len(judged)) if judged else None,
        "deletion_only_policy": deletion_only,
        "id_survival_agreement": (sum(agree) / len(agree)) if agree else None,
        "id_survival_agreement_llm_only": (sum(agree_llm) / len(agree_llm))
                                          if agree_llm else None,
        **monitor.as_dict(),
    }


# ── E2 mode ─────────────────────────────────────────────────────────

def _gold_texts_by_qid(config_dir: Path) -> dict[str, list[str]]:
    if not (config_dir / "gold_answer_snapshot.jsonl").exists():
        return {}
    by_qid: dict[str, list[str]] = defaultdict(list)
    for row in load_snapshot_rows(config_dir):
        t = (row.get("original_text") or "").strip()
        if t:
            by_qid[row["query_id"]].append(t)
    return by_qid


def run_e2(config_dir: Path, client, cache: JudgeCache, *,
           limit: int | None, honeypot_rate: float, monitor: HoneypotMonitor,
           rng: random.Random) -> list[dict]:
    results = load_results_rows(config_dir)
    gold_texts = _gold_texts_by_qid(config_dir)
    refs_pool = [r["ref_answer"] for r in results if (r.get("ref_answer") or "").strip()]

    out: list[dict] = []
    llm_calls = 0
    pot_every = max(1, int(round(1.0 / honeypot_rate))) if honeypot_rate > 0 else 0
    pot_flip = True

    for r in results:
        if limit is not None and len(out) >= limit:
            break
        question = r.get("question_text", "")
        qtype = r.get("question_type", "")
        ref = (r.get("ref_answer") or "").strip()
        answer = (r.get("reader_answer") or "").strip()

        base = {
            "query_id": r["query_id"], "question_type": qtype,
            "abstained": r.get("abstained"), "qa_correct_det": r.get("qa_correct"),
            "answer_len_chars": len(answer), "answer_len_words": len(answer.split()),
            "is_honeypot": False, "honeypot_expected": None,
            "judge_model": client.model, "prompt_version": PROMPT_VERSION,
            **_meta_from(r),
        }

        if not answer:
            out.append({**base, "verdict": None, "rule": "no_reader_answer",
                        "parse_error": False, "cache_hit": False})
            continue

        if ref:
            prompt = build_e2_prompt(question, qtype, ref, answer)
            rule = "llm"
        elif gold_texts.get(r["query_id"]):
            prompt = build_e2_goldtext_prompt(question, qtype,
                                              gold_texts[r["query_id"]], answer)
            rule = "llm_goldtext"
        else:
            out.append({**base, "verdict": None, "rule": "no_reference",
                        "parse_error": False, "cache_hit": False})
            continue

        if pot_every and llm_calls and llm_calls % pot_every == 0 and ref:
            if pot_flip:
                pot_answer, expected = ref, "CORRECT"        # verbatim reference
            else:
                donor = rng.choice(refs_pool) if refs_pool else ""
                pot_answer, expected = donor, ("INCORRECT" if donor and donor != ref else None)
            if expected:
                v, perr, chit = judge_one(
                    client, build_e2_prompt(question, qtype, ref, pot_answer),
                    E2_ALLOWED, cache)
                monitor.record(v == expected)
                out.append({**base, "verdict": v, "rule": "llm",
                            "is_honeypot": True, "honeypot_expected": expected,
                            "answer_len_chars": len(pot_answer),
                            "answer_len_words": len(pot_answer.split()),
                            "parse_error": perr, "cache_hit": chit})
            pot_flip = not pot_flip

        v, perr, chit = judge_one(client, prompt, E2_ALLOWED, cache)
        llm_calls += 1
        out.append({**base, "verdict": v, "rule": rule,
                    "parse_error": perr, "cache_hit": chit})
    return out


def summarize_e2(rows: list[dict], monitor: HoneypotMonitor) -> dict:
    real = [r for r in rows if not r["is_honeypot"]]
    judged = [r for r in real if r["verdict"] in E2_ALLOWED]
    by_type: dict[str, dict] = {}
    for qt in sorted({r["question_type"] for r in judged}):
        sub = [r for r in judged if r["question_type"] == qt]
        cor = [r for r in sub if r["verdict"] == "CORRECT"]
        inc = [r for r in sub if r["verdict"] == "INCORRECT"]
        # Verbosity-bias raw material: length by verdict within type.
        by_type[qt] = {
            "n": len(sub),
            "judge_correct_rate": len(cor) / len(sub),
            "mean_len_correct": (sum(r["answer_len_words"] for r in cor) / len(cor))
                                if cor else None,
            "mean_len_incorrect": (sum(r["answer_len_words"] for r in inc) / len(inc))
                                  if inc else None,
        }
    det_pairs = [(r["verdict"] == "CORRECT", str(r.get("qa_correct_det")) == "True")
                 for r in judged]
    return {
        "mode": "e2", "prompt_version": PROMPT_VERSION,
        "judge_model": rows[0]["judge_model"] if rows else None,
        "n_rows": len(real),
        "n_judged": len(judged),
        "n_goldtext_graded": sum(1 for r in real if r["rule"] == "llm_goldtext"),
        "n_no_reference": sum(1 for r in real if r["rule"] == "no_reference"),
        "n_no_reader_answer": sum(1 for r in real if r["rule"] == "no_reader_answer"),
        "n_parse_errors": sum(1 for r in real if r["parse_error"]),
        "judge_correct_rate": (sum(1 for r in judged if r["verdict"] == "CORRECT")
                               / len(judged)) if judged else None,
        "det_grader_agreement": (sum(int(a == b) for a, b in det_pairs)
                                 / len(det_pairs)) if det_pairs else None,
        "by_question_type": by_type,
        **monitor.as_dict(),
    }


# ── validation utilities ────────────────────────────────────────────

def cohens_kappa(a: list[str], b: list[str]) -> float:
    assert len(a) == len(b) and a, "label lists must be equal-length, non-empty"
    labels = sorted(set(a) | set(b))
    po = sum(1 for x, y in zip(a, b) if x == y) / len(a)
    pe = sum((a.count(l) / len(a)) * (b.count(l) / len(b)) for l in labels)
    return (po - pe) / (1 - pe) if pe < 1 else 1.0


def export_validation_sample(all_rows: list[dict], mode: str, path: Path,
                             n: int, rng: random.Random) -> None:
    """Stratified (question_type × verdict) blinded sample for hand-labeling.
    Blinded CSV has NO policy/budget/verdict columns; the .key.csv next to it
    holds the mapping + judge verdicts for scoring with --score-validation."""
    pool = [r for r in all_rows
            if r["rule"].startswith("llm") and not r["is_honeypot"]
            and r["verdict"] is not None]
    strata: dict[tuple, list[dict]] = defaultdict(list)
    for r in pool:
        strata[(r["question_type"], r["verdict"])].append(r)
    take: list[dict] = []
    keys = sorted(strata)
    while len(take) < min(n, len(pool)):
        for k in keys:
            if strata[k] and len(take) < min(n, len(pool)):
                take.append(strata[k].pop(rng.randrange(len(strata[k]))))
    rng.shuffle(take)

    blind_cols = (["sample_id", "question", "gold_answer", "memory_text", "human_label"]
                  if mode == "e1" else
                  ["sample_id", "question", "reference", "answer", "human_label"])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(blind_cols)
        for i, r in enumerate(take):
            if mode == "e1":
                w.writerow([i, r["_question"], r["_gold_answer"], r["_text"], ""])
            else:
                w.writerow([i, r["_question"], r["_reference"], r["_answer"], ""])
    with Path(str(path) + ".key.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sample_id", "config_dir", "query_id", "gold_id", "verdict"])
        for i, r in enumerate(take):
            w.writerow([i, r["_config_dir"], r["query_id"], r.get("gold_id", ""),
                        r["verdict"]])
    print(f"validation sample: {len(take)} blinded rows → {path} "
          f"(key: {path}.key.csv — labels: "
          f"{'YES/NO' if mode == 'e1' else 'CORRECT/INCORRECT'})")


def score_validation(labeled_csv: Path, key_csv: Path) -> None:
    with labeled_csv.open(encoding="utf-8") as f:
        human = {r["sample_id"]: r["human_label"].strip().upper()
                 for r in csv.DictReader(f) if r.get("human_label", "").strip()}
    with key_csv.open(encoding="utf-8") as f:
        judge = {r["sample_id"]: r["verdict"] for r in csv.DictReader(f)}
    ids = sorted(set(human) & set(judge))
    if not ids:
        raise SystemExit("no overlapping labeled rows between the two files.")
    h = [human[i] for i in ids]
    j = [judge[i] for i in ids]
    kappa = cohens_kappa(h, j)
    agree = sum(1 for x, y in zip(h, j) if x == y) / len(ids)
    print(f"n={len(ids)}  raw agreement={agree:.3f}  Cohen's κ={kappa:.3f}")
    print("MEMAUDIT bar for reference: human–judge κ ≈ 0.86.")


# ── driver ──────────────────────────────────────────────────────────

def _judge_config(mode: str, config_dir: Path, client, cache: JudgeCache,
                  args, rng: random.Random) -> list[dict]:
    monitor = HoneypotMonitor(args.honeypot_threshold, args.honeypot_min)
    runner = run_e1 if mode == "e1" else run_e2
    rows = runner(config_dir, client, cache, limit=args.limit,
                  honeypot_rate=args.honeypot_rate, monitor=monitor, rng=rng)
    summary = (summarize_e1 if mode == "e1" else summarize_e2)(rows, monitor)

    out_path = config_dir / f"judge_{mode}.jsonl"
    with out_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({k: v for k, v in r.items()
                                if not k.startswith("_")}) + "\n")
    (config_dir / f"judge_{mode}_summary.json").write_text(
        json.dumps(summary, indent=2))
    log.info("%s: %d rows → %s (honeypot pass %s)", config_dir, len(rows),
             out_path.name, summary.get("honeypot_pass_rate"))
    return rows


def _attach_validation_fields(mode: str, rows: list[dict],
                              config_dir: Path) -> None:
    """Re-derive the blinded text fields for the validation export (underscore
    fields are stripped from judge_*.jsonl)."""
    results = {r["query_id"]: r for r in load_results_rows(config_dir)}
    snap = ({(r["query_id"], r["gold_id"]): r for r in load_snapshot_rows(config_dir)}
            if (config_dir / "gold_answer_snapshot.jsonl").exists() else {})
    for r in rows:
        rr = results.get(r["query_id"], {})
        r["_config_dir"] = str(config_dir)
        r["_question"] = rr.get("question_text", "")
        if mode == "e1":
            s = snap.get((r["query_id"], r.get("gold_id")), {})
            r["_gold_answer"] = s.get("answer", "")
            r["_text"] = s.get("resident_text", "")
        else:
            r["_reference"] = rr.get("ref_answer", "")
            r["_answer"] = rr.get("reader_answer", "")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description="Offline judge over run artifacts (E1 recoverability / "
                    "E2 answer re-grade). See module docstring.")
    p.add_argument("--mode", choices=["e1", "e2"],
                   help="e1 = recoverability bracket over gold_answer_snapshot; "
                        "e2 = re-grade of recorded reader answers.")
    p.add_argument("--runs", nargs="+", metavar="GLOB",
                   help="Glob(s) of config dirs (dirs containing results.csv), "
                        "e.g. 'artifacts/benchmark_full/beam/**/PlainTextReader'.")
    p.add_argument("--judge-model", default=os.getenv("JUDGE_MODEL"),
                   help="Judge model name (env JUDGE_MODEL). Family MUST differ "
                        "from the reader family. 'mock' = offline stub for "
                        "plumbing dry-runs.")
    p.add_argument("--judge-base-url", default=None,
                   help="OpenAI-compatible endpoint (env JUDGE_BASE_URL; "
                        "defaults to LLM_BASE_URL).")
    p.add_argument("--judge-api-key", default=None)
    p.add_argument("--limit", type=int, default=None,
                   help="Cap output rows per config — cheap dry-runs.")
    p.add_argument("--cache-path", default=os.getenv(
        "JUDGE_CACHE_PATH", "artifacts/judge_cache.jsonl"))
    p.add_argument("--honeypot-rate", type=float, default=0.1,
                   help="Honeypots per real LLM call (0 disables).")
    p.add_argument("--honeypot-threshold", type=float, default=0.8,
                   help="Abort the batch below this running pass rate.")
    p.add_argument("--honeypot-min", type=int, default=10,
                   help="Honeypots before the threshold is enforced.")
    p.add_argument("--export-validation", metavar="CSV", default=None,
                   help="After judging, export a stratified blinded sample for "
                        "hand-labeling (plus a .key.csv for scoring).")
    p.add_argument("--validation-n", type=int, default=None,
                   help="Sample size (default: 100 for e1, 150 for e2).")
    p.add_argument("--second-judge", metavar="MODEL", default=None,
                   help="Re-judge a fraction of rows under a second judge "
                        "family; reports inter-judge κ.")
    p.add_argument("--second-judge-frac", type=float, default=0.1)
    p.add_argument("--score-validation", metavar="LABELED_CSV", default=None,
                   help="Compute Cohen's κ from a returned hand-labeled sample "
                        "(requires --validation-key). No judging happens.")
    p.add_argument("--validation-key", metavar="KEY_CSV", default=None)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                        datefmt="%H:%M:%S")

    if args.score_validation:
        if not args.validation_key:
            p.error("--score-validation requires --validation-key")
        score_validation(Path(args.score_validation), Path(args.validation_key))
        return

    if not args.mode or not args.runs:
        p.error("--mode and --runs are required (unless --score-validation)")
    if not args.judge_model:
        p.error("no judge model: pass --judge-model or set JUDGE_MODEL")

    from forgetit.settings import LLM_MODEL
    check_family_separation(args.judge_model, LLM_MODEL or "")
    if args.second_judge:
        check_family_separation(args.second_judge, LLM_MODEL or "")
        if (model_family(args.second_judge) == model_family(args.judge_model)
                and args.judge_model != "mock"):
            raise SystemExit("--second-judge must be a DIFFERENT family from "
                             "--judge-model (inter-family agreement is the point).")

    client = build_judge_client(args.judge_model, args.judge_base_url,
                                args.judge_api_key)
    cache = JudgeCache(args.cache_path)
    rng = random.Random(_SEED)

    dirs = find_config_dirs(args.runs, need_snapshot=(args.mode == "e1"))
    if not dirs:
        raise SystemExit(f"--runs matched no config dirs: {args.runs}")
    log.info("judging %d config dir(s), mode=%s, judge=%s, prompt=%s",
             len(dirs), args.mode, args.judge_model, PROMPT_VERSION)

    all_rows: list[dict] = []
    for d in dirs:
        rows = _judge_config(args.mode, d, client, cache, args, rng)
        _attach_validation_fields(args.mode, rows, d)
        all_rows.extend(rows)

    if args.export_validation:
        n = args.validation_n or (100 if args.mode == "e1" else 150)
        export_validation_sample(all_rows, args.mode,
                                 Path(args.export_validation), n, rng)

    if args.second_judge:
        client2 = build_judge_client(args.second_judge, args.judge_base_url,
                                     args.judge_api_key)
        pool = [r for r in all_rows if r["rule"].startswith("llm")
                and not r["is_honeypot"] and r["verdict"] is not None]
        k = max(1, int(len(pool) * args.second_judge_frac))
        sample = rng.sample(pool, k)
        allowed = E1_ALLOWED if args.mode == "e1" else E2_ALLOWED
        pairs = []
        second_rows = []
        for r in sample:
            if args.mode == "e1":
                prompt = build_e1_prompt(r["_question"], r["_gold_answer"], r["_text"])
            elif r["rule"] == "llm_goldtext":
                continue        # gold-text prompts need the snapshot join; skip in κ
            else:
                prompt = build_e2_prompt(r["_question"], r["question_type"],
                                         r["_reference"], r["_answer"])
            v2, perr, _ = judge_one(client2, prompt, allowed, cache)
            if v2 is not None and not perr:
                pairs.append((r["verdict"], v2))
                second_rows.append({"query_id": r["query_id"],
                                    "gold_id": r.get("gold_id", ""),
                                    "config_dir": r["_config_dir"],
                                    "verdict_primary": r["verdict"],
                                    "verdict_second": v2,
                                    "judge_model_second": client2.model,
                                    "prompt_version": PROMPT_VERSION})
        if pairs:
            a, b = [x for x, _ in pairs], [y for _, y in pairs]
            kappa = cohens_kappa(a, b)
            agree = sum(1 for x, y in pairs if x == y) / len(pairs)
            out = Path(dirs[0]) / f"judge_{args.mode}_second.jsonl"
            with out.open("w", encoding="utf-8") as f:
                for r in second_rows:
                    f.write(json.dumps(r) + "\n")
            print(f"inter-judge: n={len(pairs)}  agreement={agree:.3f}  "
                  f"κ={kappa:.3f}  ({args.judge_model} vs {args.second_judge}) "
                  f"→ {out}")
        else:
            print("inter-judge: no comparable rows in the sample.")

    cache.close()
    print(f"done: {len(all_rows)} judge rows across {len(dirs)} config(s).")


if __name__ == "__main__":
    main()
