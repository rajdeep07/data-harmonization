from typing import Optional

from data_harmonization.main.code.tiger.model.datamodel import *
from data_harmonization.main.code.tiger.transformer.utils.StringSupport import \
    StringSupport


class CityTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    @staticmethod
    def standardizeCity(value: str):
        return (
            StringSupport().normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters(
                value
            )
        )
        # return "".join(list(map(
        #    lambda x: StringSupport().normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters(x),
        #    list(value))))
