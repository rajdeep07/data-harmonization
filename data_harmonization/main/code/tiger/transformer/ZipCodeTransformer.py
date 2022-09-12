from data_harmonization.main.code.com.tiger.data.transformer.utils import StringSupport
from data_harmonization.main.code.com.tiger.data.model.datamodel import *
from typing import Optional


class ZipCodeTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)


    def standardizeZipCode(self, value: str):
        value.map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)



