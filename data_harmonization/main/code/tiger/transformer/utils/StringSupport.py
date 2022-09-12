from typing import Optional
from cleantext import clean


class StringSupport:
    def toOptional(self, value: str) -> Optional[str]:
        if value.nonEmpty():
            return value
        else:
            return None

    def trimAndLowerCase(self, value: str) -> str:
        value.strip().lower()

    def normalizeString(self, value: str) -> str:
        clean(value, normalize_whitespace=True)

    def normalizeTrimAndLowerCaseString(self, value: str) -> str:
        normalizeString(trimAndLowerCase(value))

    def normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters(self, value: str) -> str:
        normalizeTrimAndLowerCaseString(value)\
            .replace("[$&+_~`,:;=.,!?@#|'<>.^*()\\[\\]%!\\-/{}ƜɯƟɵƢƣƻƼƽƾǀǁǂǃǄǅǆǇǈǉǊǋǌǰǱǲǳ]", "")\
            .replace(" +", " ")

    def normalizeTrimAndLowerCaseStringAndRemoveNumbers(self, value: str) -> str:
        normalizeTrimAndLowerCaseString(value).replace("[0123456789]", "")

    def consolidateWhiteSpacesAndNewLines(self, value:str) -> str:
        value.strip().replace("[ \\r\\n]+", " ")