from typing import Optional

from data_harmonization.main.code.tiger.model.datamodel import *
from data_harmonization.main.code.tiger.transformer.utils.StringSupport import \
    StringSupport


class StateTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    @staticmethod
    def standardizeState(value: str):
        value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)
