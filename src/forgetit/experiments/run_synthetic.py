from __future__ import annotations

from forgetit.benchmark.pipeline import run_pipeline
from forgetit.benchmark.workloads.synthetic import SyntheticWorkload
from forgetit.policies import LRUPolicy
from forgetit.policies import LFUPolicy


def main() -> None:
    # Materialize once so the workload can estimate total text bytes.
    workload = SyntheticWorkload()
    list(workload.events())

    budgets = workload.suggested_budgets()
    if not budgets:
        raise RuntimeError("Synthetic workload produced no budget suggestions.")

    policies = [
        # TODO ("fifo", FIFOPolicy()),
        ("lru", LRUPolicy()),
        ("lfu", LFUPolicy()),
    ]

    print("Synthetic workload budgets:", budgets)
    print()

    for policy_name, policy in policies:
        print(f"=== Policy: {policy_name} ===")
        for budget in budgets:
            results, summary = run_pipeline(
                dataset="synthetic",
                workload=workload,
                budget_bytes=budget,
                policy=policy,
                policy_name=policy_name,
                k=5,
                seed=42,
                itemiser="none",
                reader="none",
                out_dir="artifacts/benchmark",
            )

            print(
                f"budget={budget:<6} "
                f"n={int(summary['n_queries']):<2} "
                f"gold_survival={summary['gold_survival_rate']:.3f} "
                f"ubf={summary['useful_byte_fraction']:.3f} "
                f"waste={summary['waste_rate']:.3f} "
                f"hit@k={summary['hit_at_k']:.3f} "
                f"hit@k|retained={summary['hit_at_k_given_retained']:.3f} "
                f"mrr={summary['mean_mrr']:.3f}"
            )
        print()


if __name__ == "__main__":
    main()