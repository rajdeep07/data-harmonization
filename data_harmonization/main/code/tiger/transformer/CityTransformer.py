from data_harmonization.main.code.tiger.transformer.utils.StringSupport import *
from data_harmonization.main.code.tiger.model.datamodel import *
from typing import Optional


class CityTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    def standardizeCity(self, value: str):
        value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)



