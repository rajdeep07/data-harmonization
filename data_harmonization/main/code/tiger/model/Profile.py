from dataclasses import dataclass
from typing import Optional


@dataclass(unsafe_hash=True)
class Profile:
    id: str
    name: str
    city: str
    address: str
    state: str
    zipcode: str
