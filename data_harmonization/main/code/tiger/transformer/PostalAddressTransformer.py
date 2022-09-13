from data_harmonization.main.code.tiger.transformer.utils.StringSupport import *
from data_harmonization.main.code.tiger.model.datamodel import *
from typing import Optional


class PostalAddressTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    def standardizePostalAddress(self, value: str):
        return value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)



