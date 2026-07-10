"""Judge system (E1/E2): parser robustness, cache behavior, honeypot
accounting, rule rows, and — most important — prompt blindness to
policy/budget/config labels."""
import csv
import json

import pytest

from forgetit.benchmark.judge import (
    E1_ALLOWED,
    E2_ALLOWED,
    HoneypotMonitor,
    JudgeCache,
    MockJudge,
    cohens_kappa,
    judge_one,
    model_family,
    parse_verdict,
    run_e1,
    run_e2,
)
from forgetit.benchmark.judge_prompts import build_e1_prompt
import random


class ScriptedClient:
    model = "scripted-judge"

    def __init__(self, replies=None, default="VERDICT: YES\nok."):
        self.replies = list(replies or [])
        self.default = default
        self.prompts = []

    def generate(self, prompt, **kw):
        self.prompts.append(prompt)
        return self.replies.pop(0) if self.replies else self.default


# ── parser ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("raw,allowed,want", [
    ("VERDICT: YES\nbecause it is there.", E1_ALLOWED, "YES"),
    ("verdict: no", E1_ALLOWED, "NO"),
    ("**VERDICT: CORRECT** — matches.", E2_ALLOWED, "CORRECT"),
    ("  VERDICT - INCORRECT", E2_ALLOWED, "INCORRECT"),
    ("NO.", E1_ALLOWED, "NO"),                       # bare token fallback
    ("VERDICT: MAYBE", E1_ALLOWED, None),            # not in allowed set
    ("I think the answer survives.", E1_ALLOWED, None),
    ("", E1_ALLOWED, None),
])
def test_parse_verdict(raw, allowed, want):
    assert parse_verdict(raw, allowed) == want


def test_judge_one_retries_once_on_unparseable(tmp_path):
    cache = JudgeCache(tmp_path / "c.jsonl")
    client = ScriptedClient(replies=["rambling with no verdict",
                                     "VERDICT: NO"])
    v, perr, hit = judge_one(client, "p1", E1_ALLOWED, cache)
    assert (v, perr, hit) == ("NO", False, False)
    assert len(client.prompts) == 2                  # original + re-ask
    assert "could not be parsed" in client.prompts[1]

    client2 = ScriptedClient(default="still garbage")
    v, perr, _ = judge_one(client2, "p2", E1_ALLOWED, cache)
    assert v is None and perr is True                # gave up after one retry


# ── cache ────────────────────────────────────────────────────────────


def test_cache_hit_skips_llm_and_survives_restart(tmp_path):
    path = tmp_path / "cache.jsonl"
    cache = JudgeCache(path)
    client = ScriptedClient()
    judge_one(client, "same prompt", E1_ALLOWED, cache)
    assert len(client.prompts) == 1

    v, perr, hit = judge_one(client, "same prompt", E1_ALLOWED, cache)
    assert hit is True and v == "YES"
    assert len(client.prompts) == 1                  # no second call
    cache.close()

    cache2 = JudgeCache(path)                        # resume from disk
    v, perr, hit = judge_one(client, "same prompt", E1_ALLOWED, cache2)
    assert hit is True and len(client.prompts) == 1


def test_cache_key_includes_model():
    assert JudgeCache.key("model-a", "p") != JudgeCache.key("model-b", "p")


# ── honeypots ────────────────────────────────────────────────────────


def test_honeypot_monitor_aborts_below_threshold():
    m = HoneypotMonitor(threshold=0.8, min_count=5)
    for _ in range(4):
        m.record(True)                               # under min_count: no abort
    m.record(False)                                  # 4/5 = 0.8, at threshold
    with pytest.raises(SystemExit, match="HONEYPOT FAILURE"):
        m.record(False)                              # 4/6 < 0.8 → abort


# ── synthetic config dir ─────────────────────────────────────────────

POLICY = "zz_secret_policy"
RETRIEVER = "zz_secret_retriever"
STORE = f"retention/{POLICY}/{RETRIEVER}/budget_007pct"
FORBIDDEN = [POLICY, RETRIEVER, STORE, "budget_007pct", "9977553",
             "probes_blind", "zz_itemiser"]


def make_config_dir(tmp_path):
    d = tmp_path / "cfg"
    d.mkdir()
    res_cols = ["dataset", "query_id", "policy", "budget_bytes", "budget_frac",
                "probe_touch_mode", "reader_token_budget", "itemiser",
                "retriever", "store", "question_text", "question_type",
                "ref_answer", "reader_answer", "abstained", "qa_correct"]
    rows = [
        dict(dataset="test", query_id="q1", policy=POLICY, budget_bytes="9977553",
             budget_frac="0.07", probe_touch_mode="probes_blind",
             reader_token_budget="375", itemiser="zz_itemiser",
             retriever=RETRIEVER, store=STORE,
             question_text="What color is the boat?", question_type="single",
             ref_answer="red", reader_answer="The boat is red.",
             abstained="False", qa_correct="True"),
        dict(dataset="test", query_id="q2", policy=POLICY, budget_bytes="9977553",
             budget_frac="0.07", probe_touch_mode="probes_blind",
             reader_token_budget="375", itemiser="zz_itemiser",
             retriever=RETRIEVER, store=STORE,
             question_text="Where did Ana move?", question_type="single",
             ref_answer="", reader_answer="Ana moved to Lisbon.",
             abstained="False", qa_correct="False"),
    ]
    with (d / "results.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=res_cols)
        w.writeheader()
        w.writerows(rows)
    snap = [
        dict(example_id="e1", query_id="q1", gold_id="g1", answer="red",
             question_type="single",
             original_text="user: my boat is red and fast",
             orig_bytes=29, resident_text="user: my boat is red and fast",
             resident_bytes=29, was_evicted=False, was_compressed=False),
        dict(example_id="e1", query_id="q1", gold_id="g2", answer="red",
             question_type="single", original_text="user: painting day",
             orig_bytes=18, resident_text="", resident_bytes=0,
             was_evicted=True, was_compressed=False),
        dict(example_id="e2", query_id="q2", gold_id="g3", answer="",
             question_type="single",
             original_text="user: Ana moved to Lisbon last spring",
             orig_bytes=37, resident_text="user: Ana moved to Lisbon last spring",
             resident_bytes=37, was_evicted=False, was_compressed=False),
    ]
    with (d / "gold_answer_snapshot.jsonl").open("w") as f:
        for r in snap:
            f.write(json.dumps(r) + "\n")
    return d


def run_mode(mode, tmp_path, client, honeypot_rate=0.0):
    d = make_config_dir(tmp_path)
    cache = JudgeCache(tmp_path / "cache.jsonl")
    monitor = HoneypotMonitor()
    fn = run_e1 if mode == "e1" else run_e2
    rows = fn(d, client, cache, limit=None, honeypot_rate=honeypot_rate,
              monitor=monitor, rng=random.Random(0))
    return rows, client, monitor


# ── E1 semantics ─────────────────────────────────────────────────────


def test_e1_rules_and_verdicts(tmp_path):
    rows, client, _ = run_mode("e1", tmp_path, ScriptedClient())
    by_gold = {r["gold_id"]: r for r in rows}
    assert by_gold["g1"]["rule"] == "llm" and by_gold["g1"]["verdict"] == "YES"
    assert by_gold["g2"]["rule"] == "evicted_empty" and by_gold["g2"]["verdict"] == "NO"
    assert by_gold["g3"]["rule"] == "no_reference" and by_gold["g3"]["verdict"] is None
    assert len(client.prompts) == 1                  # only g1 hit the LLM
    assert all(r["judge_model"] == "scripted-judge" for r in rows)
    assert all(r["prompt_version"] for r in rows)


def test_e1_honeypots_flagged_and_counted(tmp_path):
    d = make_config_dir(tmp_path)
    # Two judgeable rows needed for a pot to fire between them → add one.
    with (d / "gold_answer_snapshot.jsonl").open("a") as f:
        f.write(json.dumps(dict(
            example_id="e9", query_id="q1", gold_id="g9", answer="red",
            question_type="single", original_text="user: crimson boat",
            orig_bytes=18, resident_text="user: the boat is red",
            resident_bytes=21, was_evicted=False, was_compressed=False)) + "\n")
    cache = JudgeCache(tmp_path / "c2.jsonl")
    monitor = HoneypotMonitor()
    rows = run_e1(d, ScriptedClient(), cache, limit=None, honeypot_rate=1.0,
                  monitor=monitor, rng=random.Random(0))
    pots = [r for r in rows if r["is_honeypot"]]
    assert pots, "no honeypot fired at rate 1.0"
    assert all(r["honeypot_expected"] in ("YES", "NO") for r in pots)
    assert monitor.total == len(pots)


# ── E2 semantics ─────────────────────────────────────────────────────


def test_e2_reference_and_goldtext_paths(tmp_path):
    rows, client, _ = run_mode("e2", tmp_path, MockJudge())
    by_qid = {r["query_id"]: r for r in rows}
    assert by_qid["q1"]["rule"] == "llm"             # has a reference
    assert by_qid["q1"]["verdict"] == "CORRECT"      # 'red' contained
    assert by_qid["q2"]["rule"] == "llm_goldtext"    # empty ref → gold text
    assert by_qid["q2"]["verdict"] == "CORRECT"      # Lisbon in gold text
    assert by_qid["q1"]["answer_len_words"] == 4     # verbosity raw material


# ── blindness (the load-bearing test) ────────────────────────────────


@pytest.mark.parametrize("mode", ["e1", "e2"])
def test_prompts_blind_to_policy_budget_config(mode, tmp_path):
    rows, client, _ = run_mode(mode, tmp_path, ScriptedClient(), honeypot_rate=0.5)
    assert client.prompts, "nothing was judged"
    for prompt in client.prompts:
        for token in FORBIDDEN:
            assert token not in prompt, (
                f"config label {token!r} leaked into a {mode} prompt")
    # ...while the OUTPUT rows must carry the labels for offline joins.
    llm_rows = [r for r in rows if r["rule"].startswith("llm")]
    assert all(r["policy"] == POLICY for r in llm_rows)
    assert all(r["probe_touch_mode"] == "probes_blind" for r in llm_rows)


# ── misc ─────────────────────────────────────────────────────────────


def test_mock_judge_e1_containment():
    yes = MockJudge().generate(build_e1_prompt("Q?", "red", "the boat is red"))
    no = MockJudge().generate(build_e1_prompt("Q?", "red", "a blue kayak"))
    assert parse_verdict(yes, E1_ALLOWED) == "YES"
    assert parse_verdict(no, E1_ALLOWED) == "NO"


def test_cohens_kappa():
    assert cohens_kappa(["YES", "NO"] * 5, ["YES", "NO"] * 5) == 1.0
    assert abs(cohens_kappa(["YES", "NO"] * 5, ["NO", "YES"] * 5) - (-1.0)) < 1e-9


def test_model_family():
    assert model_family("qwen2.5:7b-instruct") == "qwen"
    assert model_family("meta-llama/Llama-3.1-8B") == "llama"
    assert model_family("mixtral-8x7b") == "mistral"
    assert model_family("weird-model") == "unknown"
