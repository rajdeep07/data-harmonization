from data_harmonization.main.code.tiger.transformer.utils.StringSupport import StringSupport
from data_harmonization.main.code.tiger.model.datamodel import *
from typing import Optional


class CityTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    @staticmethod
    def standardizeCity(value: str):
        return "".join(list(map(
            lambda x: StringSupport().normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters(x),
            list(value))))
        #return value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)



