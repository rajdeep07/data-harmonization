from typing import Optional
from data_harmonization.main.code.com.tiger.data.model.datamodel import *

class IntegerTypeTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    def standardizeIntegerType(self, value: str):
        value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)