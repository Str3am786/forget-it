#!/usr/bin/env python3
"""
Stage the reader roster onto LUMI scratch and pin its provenance.

RUN THIS ON A LOGIN NODE. Compute nodes have no outbound network, which is why
every launcher exports HF_HUB_OFFLINE=1; a model that was not staged here will
fail at server start rather than silently download.

What it does, per reader in readers.json:
  1. resolves the repo to a commit sha (so 'main' moving later cannot change
     what a rerun serves),
  2. downloads that exact revision into $MODELS_DIR/<id> as real files, not a
     symlink farm, because the HF cache layout behaves poorly on Lustre and
     because a directory of real files is what gets archived with the artefact,
  3. writes readers.lock.json recording sha, byte count, file count and the
     resolved local path.

The lock file is the thing that goes in the appendix. readers.json says what you
intended to run; readers.lock.json says what you actually ran.

Usage
-----
    export HF_TOKEN=hf_xxx                 # needed for the gated repos
    export MODELS_DIR=/scratch/project_465XXXXXX/models
    python3 fetch_readers.py --manifest configs/readers.json

    python3 fetch_readers.py --manifest configs/readers.json --check
        # verifies an existing staging without downloading anything

Gated repos (Llama 3.1, Gemma 3) require that the account behind HF_TOKEN has
accepted the licence on the model page first. Approval is not always instant, so
do that before the day you plan to run.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

try:
    from huggingface_hub import (
        HfApi,
        get_hf_file_metadata,
        hf_hub_url,
        snapshot_download,
    )
except ImportError:  # pragma: no cover
    sys.exit(
        "huggingface_hub is not importable.\n"
        "  python3 -m pip install --user 'huggingface_hub[cli]>=0.24'"
    )


# Weight shards and the metadata needed to load them. Deliberately excludes
# *.pth, *.bin and *.gguf: the safetensors copy is authoritative and pulling the
# duplicate formats roughly doubles the transfer for nothing.
ALLOW = [
    "*.safetensors",
    "*.safetensors.index.json",
    "config.json",
    "generation_config.json",
    "tokenizer*",
    "vocab*",
    "merges.txt",
    "special_tokens_map.json",
    "preprocessor_config.json",
    "chat_template.*",
    "*.model",
    "LICENSE*",
    "README.md",
]


def dir_stats(path: Path) -> tuple[int, int]:
    """Return (file_count, total_bytes) for a staged model directory."""
    files = [p for p in path.rglob("*") if p.is_file()]
    return len(files), sum(p.stat().st_size for p in files)


def precheck_access(readers: list[dict], token: str | None) -> dict[str, dict]:
    """Resolve every repo before downloading anything.

    NOTE: metadata readability is NOT an access test. HfApi.model_info succeeds
    on a gated repo for any authenticated account, because the gate applies to
    the resolve/ file endpoints rather than to the repo record. Testing with
    model_info alone reports a gated repo as accessible and the 403 then lands
    at the first real file, which is the bug this replaces. The valid test is a
    HEAD against a file.
    """
    api = HfApi(token=token)
    out: dict[str, dict] = {}

    who = "anonymous"
    try:
        who = api.whoami(token=token)["name"]
    except Exception:
        pass
    print(f"[fetch] access precheck (authenticated as: {who})")

    for r in readers:
        rid, repo = r["id"], r["hf_repo"]
        rev = r.get("revision", "main")
        try:
            info = api.model_info(repo_id=repo, revision=rev)
        except Exception as exc:
            out[rid] = {"ok": False, "stage": "metadata", "error": f"{type(exc).__name__}: {exc}"}
            print(f"  {rid:<28}BLOCKED   metadata: {type(exc).__name__}")
            continue

        # The real test: can this token actually fetch bytes from the repo?
        probe = next(
            (f for f in ("config.json", "generation_config.json", "tokenizer_config.json")
             if any(s.rfilename == f for s in (info.siblings or []))),
            "config.json",
        )
        try:
            get_hf_file_metadata(hf_hub_url(repo, probe, revision=info.sha), token=token)
            out[rid] = {"ok": True, "sha": info.sha}
            print(f"  {rid:<28}ok        sha {info.sha[:12]}")
        except Exception as exc:
            name = type(exc).__name__
            out[rid] = {"ok": False, "stage": "files", "error": f"{name}: {exc}"}
            print(f"  {rid:<28}BLOCKED   files: {name}")
            if r.get("gated") or "Gated" in name or "403" in str(exc):
                print(f"      metadata is readable but file access is denied, so the")
                print(f"      account '{who}' is authenticated but NOT authorised.")
                print(f"      -> accept the licence at https://huggingface.co/{repo}")
                print(f"         while signed in as '{who}', not as another account")
                print("      -> fine grained tokens also need 'Read access to contents "
                      "of all public gated repos you can access'")
    print()
    return out


def stage(reader: dict, models_dir: Path, token: str | None, check_only: bool) -> dict:
    rid = reader["id"]
    repo = reader["hf_repo"]
    target = models_dir / rid
    api = HfApi(token=token)

    record = {
        "id": rid,
        "hf_repo": repo,
        "family": reader["family"],
        "band": reader["band"],
        "licence": reader["licence"],
        "local_path": str(target),
    }

    if check_only:
        if not target.is_dir():
            record["status"] = "MISSING"
            return record
        n_files, n_bytes = dir_stats(target)
        record.update(
            status="present",
            file_count=n_files,
            bytes=n_bytes,
            gib=round(n_bytes / 2**30, 2),
        )
        # A staged directory with no safetensors is a half finished download
        # that will fail at server start with a confusing error.
        if not any(target.glob("*.safetensors")):
            record["status"] = "INCOMPLETE (no safetensors)"
        return record

    print(f"[fetch] {rid}: resolving {repo} ...", flush=True)
    try:
        info = api.model_info(repo_id=repo, revision=reader.get("revision", "main"))
    except Exception as exc:  # gated, typo'd, or token missing
        record["status"] = f"RESOLVE FAILED: {type(exc).__name__}: {exc}"
        if reader.get("gated"):
            record["hint"] = (
                "This repo is gated. Accept the licence on the model page with "
                "the account behind HF_TOKEN, then retry."
            )
        return record

    sha = info.sha
    print(f"[fetch] {rid}: sha {sha[:12]} -> {target}", flush=True)

    # Contained per reader. A 403 on one gated repo must not abort the roster:
    # the ungated readers are independently useful and the sweep can start on
    # the subset that resolved while access is pending on the rest.
    try:
        local = snapshot_download(
            repo_id=repo,
            revision=sha,           # pin to the resolved commit, never to a branch
            local_dir=str(target),
            allow_patterns=ALLOW,
            token=token,
            max_workers=8,          # polite on a shared login node
        )
    except Exception as exc:
        record["status"] = f"DOWNLOAD FAILED: {type(exc).__name__}"
        record["error"] = str(exc)[:300]
        # Remove the partial staging rather than telling the user to. A half
        # staged directory is worse than an absent one: it satisfies the
        # launcher's `-d "$MODEL_PATH"` existence check and then fails at model
        # load with an error that points at vLLM instead of at the download.
        if target.is_dir() and not any(target.glob("*.safetensors")):
            import shutil

            shutil.rmtree(target, ignore_errors=True)
            record["cleanup"] = f"removed partial staging at {target}"
        return record

    n_files, n_bytes = dir_stats(Path(local))
    record.update(
        status="ok",
        revision=sha,
        file_count=n_files,
        bytes=n_bytes,
        gib=round(n_bytes / 2**30, 2),
        expected_bf16_gib=reader.get("bf16_weights_gb"),
    )
    record["chars_per_token"] = measure_density(Path(local))
    return record


# Conversational register, matching what the itemisers actually produce. Same
# text for every reader, so the ratio is comparable.
PARITY_TEXT = (
    "On Tuesday afternoon Priya mentioned that she had finally booked the flights "
    "to Porto for the last week of September, and that her brother would be joining "
    "her for the first three days before heading on to Madrid. She said the hotel "
    "near the river had been fully booked, so she settled on a smaller place two "
    "streets back, which was cheaper anyway."
) * 4


def measure_density(path: Path) -> float | None:
    """Characters per token for this checkpoint's tokeniser.

    The evidence budget is denominated in tokens and tokenisers differ across
    families, so an identical budget delivers a different amount of text to each
    reader. Measuring it here, at staging, means the comparison is a static
    property in the lock file rather than a live check that needs the pinned
    reader's server to have started first.
    """
    try:
        from transformers import AutoTokenizer
    except ImportError:
        print("  [density] transformers not importable; tokeniser parity will be "
              "unmeasured. pip install --user transformers", flush=True)
        return None
    try:
        tok = AutoTokenizer.from_pretrained(str(path), trust_remote_code=False)
        n = len(tok.encode(PARITY_TEXT, add_special_tokens=False))
        return round(len(PARITY_TEXT) / n, 3) if n else None
    except Exception as exc:
        print(f"  [density] tokeniser load failed for {path.name}: "
              f"{type(exc).__name__}: {exc}", flush=True)
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default="configs/readers.json")
    ap.add_argument("--models-dir", default=os.environ.get("MODELS_DIR"))
    ap.add_argument("--lock", default=None, help="default: alongside the manifest")
    ap.add_argument("--only", nargs="*", help="reader ids; default is all")
    ap.add_argument("--check", action="store_true", help="verify, do not download")
    ap.add_argument("--clean-incomplete", action="store_true",
                    help="delete staged directories with no safetensors before staging")
    ap.add_argument("--require-all", action="store_true",
                    help="refuse to stage a partial roster; use for the final artefact run")
    args = ap.parse_args()

    if not args.models_dir:
        return _die("set --models-dir or MODELS_DIR (e.g. /scratch/$PROJ/models)")

    manifest_path = Path(args.manifest)
    manifest = json.loads(manifest_path.read_text())
    models_dir = Path(args.models_dir)
    models_dir.mkdir(parents=True, exist_ok=True)

    token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    readers = manifest["readers"]
    if args.only:
        readers = [r for r in readers if r["id"] in set(args.only)]
        if not readers:
            return _die(f"no reader in the manifest matched {args.only}")

    gated = [r["id"] for r in readers if r.get("gated")]
    if gated and not token and not args.check:
        print(
            f"[fetch] WARNING: gated repos in this run ({', '.join(gated)}) and "
            "no HF_TOKEN in the environment. They will fail to resolve.",
            file=sys.stderr,
        )

    if args.clean_incomplete:
        for r in readers:
            d = models_dir / r["id"]
            if d.is_dir() and not any(d.glob("*.safetensors")):
                import shutil

                print(f"[fetch] removing partial staging: {d}")
                shutil.rmtree(d)

    total_expected = sum(r.get("bf16_weights_gb", 0) for r in readers)
    print(f"[fetch] {len(readers)} readers, roughly {total_expected:.0f} GB of weights")
    print(f"[fetch] target: {models_dir}")
    print("[fetch] check your quota first:  lumi-workspaces\n")

    # Resolve everything before moving a single byte, and drop the unresolvable
    # from the download pass rather than letting one 403 end the run.
    if not args.check:
        access = precheck_access(readers, token)
        blocked = [r["id"] for r in readers if not access[r["id"]]["ok"]]
        if blocked:
            print(f"[fetch] {len(blocked)} reader(s) not accessible: {', '.join(blocked)}")
            if args.require_all:
                return _die("--require-all was given; refusing to stage a partial roster")
            print("[fetch] continuing with the accessible subset.\n")
            readers = [r for r in readers if access[r["id"]]["ok"]]
            if not readers:
                return _die("nothing accessible; fix HF_TOKEN or request access first")

    records = [stage(r, models_dir, token, args.check) for r in readers]

    lock_path = Path(args.lock) if args.lock else manifest_path.with_name("readers.lock.json")
    if not args.check:
        lock_path.write_text(
            json.dumps(
                {
                    "schema_version": manifest["schema_version"],
                    "serving_window": manifest["serving_window"],
                    "dtype": manifest["dtype"],
                    "models_dir": str(models_dir),
                    "staged": records,
                },
                indent=2,
            )
            + "\n"
        )
        print(f"\n[fetch] wrote {lock_path}")

    print("\n[fetch] summary")
    print(f"  {'reader':<28}{'status':<34}{'GiB':>8}{'chars/tok':>11}")
    bad = 0
    for r in records:
        status = r.get("status", "?")
        if status not in ("ok", "present"):
            bad += 1
        cpt = r.get("chars_per_token")
        print(f"  {r['id']:<28}{status:<34}{r.get('gib', 0):>8}{(cpt if cpt else '-'):>11}")
        for k in ("hint", "remedy"):
            if k in r:
                print(f"      {k}: {r[k]}")

    # Tokeniser parity, reported here so it is visible before any job is
    # submitted rather than discovered in the results.
    ref = next((r.get("chars_per_token") for r in records
                if r["id"] == "qwen2.5-7b-instruct" and r.get("chars_per_token")), None)
    if ref:
        print(f"\n[fetch] tokeniser parity against the pinned reader ({ref} chars/token)")
        for r in records:
            cpt = r.get("chars_per_token")
            if not cpt or r["id"] == "qwen2.5-7b-instruct":
                continue
            div = abs(cpt - ref) / ref
            flag = "" if div <= 0.15 else "   <- budget is a covariate for this family"
            print(f"  {r['id']:<28}{cpt:>8}   divergence {div:5.1%}{flag}")

    if bad:
        print(f"\n[fetch] {bad} reader(s) not usable; the sweep will not run complete.")
        return 1
    print("\n[fetch] all readers staged. Next: preflight on one interactive node.")
    return 0


def _die(msg: str) -> int:
    print(f"[fetch] {msg}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
