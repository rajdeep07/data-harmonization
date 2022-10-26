import re
from typing import Optional

from cleantext import clean


class StringSupport:
    def toOptional(self, value: str) -> Optional[str]:
        if value.nonEmpty():
            return value
        else:
            return None

    def trimAndLowerCase(self, value: str) -> str:
        return value.strip().lower() if value else ""

    def normalizeString(self, value: str) -> str:
        return clean(value, normalize_whitespace=True)

    def normalizeTrimAndLowerCaseString(self, value: str) -> str:
        return self.normalizeString(self.trimAndLowerCase(value))

    def normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters(
        self, value: str
    ) -> str:
        return re.sub(
            "[\+]",
            "",
            re.sub(
                "( \+ )|( \+)",
                " ",
                re.sub(
                    "[ \+]", "", self.normalizeTrimAndLowerCaseString(value)
                ),
            ),
        )

    def normalizeTrimAndLowerCaseStringAndRemoveNumbers(
        self, value: str
    ) -> str:
        return self.normalizeTrimAndLowerCaseString(value).replace(
            "[0123456789]", ""
        )

    def consolidateWhiteSpacesAndNewLines(self, value: str) -> str:
        return value.strip().replace("[ \\r\\n]+", " ")
