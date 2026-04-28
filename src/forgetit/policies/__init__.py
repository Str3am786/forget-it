from .baselines import RandomPolicy, LRUPolicy, LFUPolicy, FIFOPolicy
from .actions import Action, Evict, Compress, Prune
from .base import EvictionPolicy, SimpleEvictionPolicy
from llm_compression import LLMCompressionPolicy
from ebbinghaus import EbbinghausPolicy

__all__ = [
    "EvictionPolicy",
    "SimpleEvictionPolicy",
    "LRUPolicy",
    "LFUPolicy", 
    "RandomPolicy", 
    "FIFOPolicy",
    "LLMCompressionPolicy",
    "EbbinghausPolicy",
    "Action", 
    "Evict",
    "Compress",
    "Prune",
    ]

# eviction / forgetting decisions
