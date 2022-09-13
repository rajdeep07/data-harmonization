from typing import Optional
from data_harmonization.main.code.tiger.model.datamodel import *
from data_harmonization.main.code.tiger.transformer.utils.StringSupport import *

class IntegerTypeTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    @staticmethod
    def standardizeIntegerType(value: str):
        return StringSupport().normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters(value)