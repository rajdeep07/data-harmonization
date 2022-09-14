from typing import Optional
#from cleantext import clean


class IntegerSupport:
    def toOptional(self, value: str) -> Optional[int]:
        if value.nonEmpty():
            return value
        else:
            return None

    def trim(self, value: str) -> int:
        return value.strip().toInt()

    def normalizeTrimAndRemoveString(self, value: str) -> int:
        return self.trim(value).replace("[a-zA-Z]", "").toInt()

