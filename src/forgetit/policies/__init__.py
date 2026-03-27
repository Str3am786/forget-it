from .baselines import RandomPolicy, LRUPolicy, LFUPolicy, FIFOPolicy
from .actions import Action, Evict, Compress, Prune
from .base import EvictionPolicy, SimpleEvictionPolicy

__all__ = [
    "EvictionPolicy",
    "SimpleEvictionPolicy"
    "LRUPolicy",
    "LFUPolicy", 
    "RandomPolicy", 
    "FIFOPolicy", 
    "Action", 
    "Evict",
    "Compress",
    "Prune"
    ]

# eviction / forgetting decisions
