from typing import Optional

from data_harmonization.main.code.tiger.transformer.utils.StringSupport import *


class StringTypeTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    @staticmethod
    def standardizeStringType(value: str):
        # value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)
        return (
            StringSupport().normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters(
                value
            )
        )
