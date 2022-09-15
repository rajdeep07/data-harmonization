from dataclasses import dataclass
from typing import Optional

@dataclass(unsafe_hash=True)
class SemiMergedProfile:
    leftId: Optional[str]
    rightId: Optional[str]
    label: Optional[float] #merged or not
    indexedLabel: Optional[float] #same
    confidence: Optional[float] # Probability of same value
