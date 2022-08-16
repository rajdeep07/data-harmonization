from data_harmonization.main.code.com.tiger.data.transformer.utils import StringSupport
from data_harmonization.main.code.com.tiger.data.model import RawProfile
from typing import Optional


class CityTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    def standardizeCity(self, value: str):
        value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)



