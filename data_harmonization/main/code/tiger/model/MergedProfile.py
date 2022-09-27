from dataclasses import dataclass
from typing import Optional


@dataclass(unsafe_hash=True)
class MergedProfile:
    id: Optional[str]
    clusterId: Optional[int]
    confidence: Optional[float]
