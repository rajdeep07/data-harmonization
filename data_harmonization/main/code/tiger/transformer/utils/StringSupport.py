from typing import Optional
#from cleantext import clean


class StringSupport:
    def toOptional(self, value: str) -> Optional[str]:
        if value.nonEmpty():
            return value
        else:
            return None

    def trimAndLowerCase(self, value: str) -> str:
        print(value)
        return value.strip().lower()

    def normalizeString(self, value: str) -> str:
        #clean(value, normalize_whitespace=True)
        return value

    def normalizeTrimAndLowerCaseString(self, value: str) -> str:
        return self.normalizeString(self.trimAndLowerCase(value))

    def normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters(self, value: str) -> str:
        return self.normalizeTrimAndLowerCaseString(value)\
            .replace("[$&+_~`,:;=.,!?@#|'<>.^*()\\[\\]%!\\-/{}ƜɯƟɵƢƣƻƼƽƾǀǁǂǃǄǅǆǇǈǉǊǋǌǰǱǲǳ]", "")\
            .replace(" +", " ")

    def normalizeTrimAndLowerCaseStringAndRemoveNumbers(self, value: str) -> str:
        return self.normalizeTrimAndLowerCaseString(value).replace("[0123456789]", "")

    def consolidateWhiteSpacesAndNewLines(self, value: str) -> str:
        return value.strip().replace("[ \\r\\n]+", " ")