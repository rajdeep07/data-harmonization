from data_harmonization.main.code.tiger.transformer.utils.StringSupport import *
from data_harmonization.main.code.tiger.model.datamodel import *
from typing import Optional


class CityTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    @staticmethod
    def standardizeCity(value: str):
        return StringSupport().normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters(value)
        # value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)



