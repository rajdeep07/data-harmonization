import re
from typing import Optional


class FloatSupport:
    def toOptional(self, value: str) -> Optional[int]:
        if value.nonEmpty():
            return value
        else:
            return None

    def trim(self, value: str) -> float:
        return float(value.strip())

    def normalizeTrimAndRemoveString(self, value: str) -> float:
        return float(re.sub("[^0-9.]", "", str(self.trim(value))))
