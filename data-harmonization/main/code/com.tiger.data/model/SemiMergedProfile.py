from dataclasses import dataclass
from typing import Optional

@dataclass(unsafe_hash=True)
class SemiMergedProfile:
    leftId: Optional[str]
    rightId: Optional[str]
    label: Optional[float]
    indexedLabel: Optional[float]
    prediction: Optional[float]
