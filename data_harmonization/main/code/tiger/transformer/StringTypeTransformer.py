from typing import Optional
from data_harmonization.main.code.com.tiger.data.transformer.utils import StringSupport
from data_harmonization.main.code.com.tiger.data.model.datamodel import *


class StringTypeTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    def standardizeStringType(self, value: str):
        value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)