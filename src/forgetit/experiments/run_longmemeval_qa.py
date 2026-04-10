from pathlib import Path
import json
import logging

from forgetit.benchmark.pipeline import run_pipeline
from forgetit.benchmark.workloads.longmemeval import (
    LongMemEvalWorkload,
    SessionItemiser,
    TurnPairItemiser,
)
from forgetit.retrieval import LexicalOverlapRetriever, BM25Retriever
from forgetit.policies import LRUPolicy, LFUPolicy
from forgetit.llm import get_llm
from forgetit.llm.readers import PlainTextReader

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "data"


def load_ref_answers(path: Path) -> dict[str, dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    refs = {}
    for ex in data:
        qid = str(ex.get("question_id", ""))
        refs[qid] = {
            "answer": ex.get("answer", ""),
            "question_type": ex.get("question_type", ""),
        }
    return refs


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    data_path = DATA_DIR / "longmemeval_s_cleaned.json"
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")

    ref_answers = load_ref_answers(data_path)
    llm = get_llm()
    reader_impl = PlainTextReader(llm)
    reader_name = getattr(reader_impl, "name", type(reader_impl).__name__)

    itemisers = [SessionItemiser(), TurnPairItemiser()]
    policies = [("lru", LRUPolicy()), ("lfu", LFUPolicy())]
    retrievers = [("lexical", LexicalOverlapRetriever()), ("bm25", BM25Retriever())]
    budgets = [262_144, 524_288, 1_048_576, 2_097_152, 5_242_880]

    for itemiser in itemisers:
        for _, policy in policies:
            for _, retriever in retrievers:
                for budget in budgets:
                    workload = LongMemEvalWorkload(data_path, itemiser)

                    _, summary = run_pipeline(
                        dataset="longmemeval",
                        workload=workload,
                        budget_bytes=budget,
                        retriever=retriever,
                        policy=policy,
                        k=5,
                        seed=42,
                        itemiser=itemiser.name,
                        reader=reader_name,
                        reader_impl=reader_impl,
                        ref_answers=ref_answers,
                        answer_max_tokens=128,
                        out_dir=REPO_ROOT / "artifacts" / "benchmark",
                    )

                    print(
                        f"itemiser={itemiser.name:<12} "
                        f"policy={getattr(policy, 'name', type(policy).__name__):<8} "
                        f"retriever={getattr(retriever, 'name', type(retriever).__name__):<8} "
                        f"reader={reader_name:<12} "
                        f"budget={budget:<10} "
                        f"gold_survival={summary['gold_survival_rate']:.3f} "
                        f"hit@k={summary['hit_at_k']:.3f}"
                    )


if __name__ == "__main__":
    main()