"""The probe scheme MUST change the config identity.

If a corrected run hashes the same as a leaky one, ``_check_done`` will match a
leaky ``DONE.json`` and the runner will silently skip the very work it was
launched to redo — the campaign completes, reports success, and the numbers are
the old contaminated ones. That failure is invisible in the logs, so it gets a
permanent test rather than a one-off check.
"""
from __future__ import annotations

from forgetit.benchmark.pipeline import _config_hash, _probe_identity
from forgetit.benchmark.probe_split import (
    PROBE_SCHEME_SPLIT, PROBE_SCHEME_SELF, PROBE_SCHEME_LEAKY,
)

BASE = dict(
    dataset="locomo", itemiser="session", store_label="retention/lfu/bm25",
    reader="qwen", k=5, seed=0, budget_bytes=262144, budget_frac=0.25,
    answer_max_tokens=128, n_examples=10, write_events=True,
    write_gold_snapshot=True,
)


class _FakeWorkload:
    """Minimal stand-in: carries a scheme, no partition manifest."""
    name = "fake"

    def __init__(self, scheme, probe_every=1):
        self.probe_scheme = scheme
        self.probe_every = probe_every


def _h(workload, tmp_path):
    return _config_hash(**BASE, **_probe_identity(workload, tmp_path))


def test_leaky_and_fixed_hash_differently(tmp_path):
    fixed = _h(_FakeWorkload(PROBE_SCHEME_SPLIT), tmp_path)
    leaky = _h(_FakeWorkload(PROBE_SCHEME_LEAKY), tmp_path)
    assert fixed != leaky, (
        "config hash ignores probe_scheme — a leaky DONE.json would satisfy a "
        "corrected config and the rerun would be silently skipped"
    )


def test_every_scheme_is_distinct(tmp_path):
    hashes = {s: _h(_FakeWorkload(s), tmp_path)
              for s in (PROBE_SCHEME_SPLIT, PROBE_SCHEME_SELF, PROBE_SCHEME_LEAKY)}
    assert len(set(hashes.values())) == 3, f"scheme hash collision: {hashes}"


def test_probe_every_is_hash_relevant(tmp_path):
    """probe_every=0 disables probing entirely (LRU/LFU collapse toward FIFO).
    That is a different experiment, not a throughput knob."""
    a = _h(_FakeWorkload(PROBE_SCHEME_SPLIT, probe_every=1), tmp_path)
    b = _h(_FakeWorkload(PROBE_SCHEME_SPLIT, probe_every=0), tmp_path)
    assert a != b


def test_identity_is_stable_across_calls(tmp_path):
    """Same workload, same hash — otherwise DONE.json never matches and every
    resubmission re-runs the whole campaign."""
    w = _FakeWorkload(PROBE_SCHEME_SPLIT)
    assert _h(w, tmp_path) == _h(w, tmp_path)


def test_legacy_workload_without_scheme_is_handled(tmp_path):
    """Workloads predating the split (synthetic) must not crash the hash."""
    class Legacy:
        name = "legacy"
    ident = _probe_identity(Legacy(), tmp_path)
    assert ident["probe_scheme"] is None
    assert _config_hash(**BASE, **ident)


def test_partition_hash_enters_identity(tmp_path):
    """A workload whose partition membership changes must hash differently,
    even at the same scheme and probe_every."""
    class WithPartition:
        name = "wp"
        probe_scheme = PROBE_SCHEME_SPLIT
        probe_every = 1

        def __init__(self, tag):
            self._tag = tag

        def write_partition_manifest(self, path):
            return f"partition-{self._tag}"

    a = _h(WithPartition("aaa"), tmp_path)
    b = _h(WithPartition("bbb"), tmp_path)
    assert a != b, "partition membership is not in the config identity"
