from dataclasses import dataclass
from typing import Optional

@dataclass(unsafe_hash=True)
class RawProfile:
    id: str
    name: Optional[str]
    city: Optional[str]
    address: Optional[str]
    state: Optional[str]
    zipcode: Optional[str]
    source: Optional[str]

