import re
from typing import Optional


class IntegerSupport:
    def toOptional(self, value: str) -> Optional[int]:
        if value.nonEmpty():
            return value
        else:
            return None

    def trim(self, value: str) -> int:
        return int(value.strip())

    def normalizeTrimAndRemoveString(self, value: str) -> int:
        return int(re.sub("[^0-9]", "", str(self.trim(value))))
