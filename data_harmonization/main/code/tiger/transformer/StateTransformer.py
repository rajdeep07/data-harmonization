from data_harmonization.main.code.tiger.transformer.utils.StringSupport import *
from data_harmonization.main.code.tiger.model.datamodel import *
from typing import Optional


class StateTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    def standardizeState(self, value: str):
        value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)



