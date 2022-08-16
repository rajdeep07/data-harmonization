from data_harmonization.main.code.com.tiger.data.transformer.utils import StringSupport
from data_harmonization.main.code.com.tiger.data.model import RawProfile
from typing import Optional


class PostalAddressTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    def standardizePostalAddress(self, value: str):
        value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)



