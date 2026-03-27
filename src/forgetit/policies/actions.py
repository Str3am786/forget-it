from __future__ import annotations

from dataclasses import dataclass, field
from typing import Union


@dataclass(frozen=True)
class Evict:
    """
    Delete this record entirely and reclaim all of its bytes
    """
    id: str

@dataclass(frozen=True)
class Compress:
    """
    Replace a record's text with a shorter version
    
    Policy is responsible of producing *new_text*. 
    The RetentionManager will overwrite the record, 
    recompute bytes 
    and credit the difference back to the budget
    """
    id: str
    new_text: str


@dataclass(frozen=True)
class Prune:
    """
    Placeholder for future graph-pruning actions
    """
    id: str
    detail: dict = field(default_factory=dict)

# ==============

Action = Union[Evict, Compress, Prune]

