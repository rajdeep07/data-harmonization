from dataclasses import dataclass
from typing import Optional


@dataclass(unsafe_hash=True)
class PostalAddress:
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    zipCode: Optional[str]
