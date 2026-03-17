from pathlib import Path
from forgetit.benchmark.pipeline import run_pipeline
from forgetit.benchmark.workloads.longmemeval import (
    LongMemEvalWorkload,
    SessionItemiser,
    TurnPairItemiser,
)
from forgetit.policies.lru import LRUPolicy
from forgetit.policies.lfu import LFUPolicy

REPO_ROOT = Path(__file__).resolve().parents[3]  
DATA_DIR = REPO_ROOT / "data"

def main() -> None:
    data_path = REPO_ROOT / "data" / "longmemeval_s_cleaned.json"
    
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")

    itemisers = [
        SessionItemiser(),
        TurnPairItemiser(),
    ]

    policies = [
        ("lru", LRUPolicy()),
        ("lfu", LFUPolicy()),
    ]

    budgets = [262_144, 524_288, 1_048_576, 2_097_152, 5_242_880]

    for itemiser in itemisers:
        for policy_name, policy in policies:
            for budget in budgets:
                workload = LongMemEvalWorkload(data_path, itemiser)

                results, summary = run_pipeline(
                    dataset="longmemeval",
                    workload=workload,
                    budget_bytes=budget,
                    policy=policy,
                    policy_name=policy_name,
                    k=5,
                    seed=42,
                    itemiser=itemiser.name,
                    reader="none",
                    out_dir="artifacts/benchmark",
                )

                print(
                    f"itemiser={itemiser.name:<12} "
                    f"policy={policy_name:<5} "
                    f"budget={budget:<10} "
                    f"gold_survival={summary['gold_survival_rate']:.3f} "
                    f"hit@k={summary['hit_at_k']:.3f}"
                )

if __name__ == "__main__":
    main()