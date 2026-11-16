# Running forget-it on LUMI

One-time setup for the campaign described in the main [README](../../README.md#reproducing-the-thesis). Everything here runs on a **login node**: it has internet, compute nodes do not.

## 1. Repo and data

Clone the repo anywhere. The launchers find it from where you submit, and expect the client image as `forgetit.sif` in the folder that holds the repo. Set `CLIENT_SIF` to use another path.

```bash
mkdir -p ~/thesis && cd ~/thesis              # any folder
git clone https://github.com/Str3am786/forget-it && cd forget-it
mkdir -p data
wget -P data/ https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json
# also place data/locomo10.json and data/beam_500K.jsonl
```

The reader sweep refuses to start if any of the three files is missing, because the reader prompt looks up session dates in them.

## 2. Client image

The client only does retrieval and HTTP calls to vLLM, so it uses the CPU base. The `forgetit` package is not installed in the image: launchers put `<repo>/src` on `PYTHONPATH`, so code edits never need a rebuild.

```bash
module load LUMI cotainr
cotainr build ../forgetit.sif --system=lumi-c --conda-env=scripts/lumi/forgetit-env.yml
```

vLLM itself comes from the LUMI AI Factory container. The reader, judge and calibration launchers pin release `lumi-multitorch-u24r70f21m50t210-20260807_115122` (`LAIF_RELEASE` in `env.sh`).

## 3. Model weights

**Readers and judge.** Stage the five readers with their exact revisions. This writes `configs/readers.lock.json`, which every launcher checks.

```bash
export HF_TOKEN=hf_xxx                         # Llama 3.1 and Gemma 3 are gated: accept their licences first
export MODELS_DIR=/scratch/project_<id>/models
python3 scripts/lumi/fetch_readers.py --manifest configs/readers.json --require-all
python3 scripts/lumi/fetch_readers.py --manifest configs/readers.json --check
```

Never use `--only`: it rewrites the lock with just that reader, and the launchers then refuse the others.

**Stage 1 models.** The state-forming sweep reads two more checkpoints by fixed path:

```bash
huggingface-cli download Qwen/Qwen2.5-7B-Instruct   --local-dir $MODELS_DIR/Qwen2.5-7B-Instruct
huggingface-cli download Qwen/Qwen3-Embedding-0.6B --local-dir $MODELS_DIR/Qwen3-Embedding-0.6B
```

## 4. Each session

```bash
export SBATCH_ACCOUNT=project_<id>     # never commit this
cd <repo>                              # submit every job from the repo root
source scripts/lumi/env.sh             # prints which paths exist
```

**The committed `configs/readers.lock.json` records the thesis runs**, including model paths on the original project's scratch. Staging the readers on a new project (step 3) rewrites it with your paths. That is expected. The original is in git history.

Then follow the stage table in the main README.

## Things that have gone wrong before

- **Compute nodes are offline.** Launchers set `HF_HUB_OFFLINE=1`, so an unstaged model fails at server start.
- **Use the ROCm container**, not a pip install of vLLM. LUMI-G is AMD MI250X.
- **`--generation-config vllm` is required.** Three of the five checkpoints ship a repetition penalty that changes greedy output. Each launcher stops if vLLM reports overridden sampling parameters.
- **LLM cache on node-local tmpfs.** One small file per call. On home it hits the 100K inode quota, and on scratch it once took 1.44M inodes.
- **Dirty tree.** Stage 1 refuses to run with uncommitted or untracked files, because every row is stamped with the commit. `FORGETIT_ALLOW_DIRTY=1` is for smoke runs only.
- **Compute-node Python is 3.6.** Run Python inside the client image, as the launchers do.
- **`env.sh` only sets defaults.** A value you export before calling a launcher always wins. An earlier version overwrote `SUBSET_ROOT`, and a judge run silently read the wrong subset.