from typing import Optional

from data_harmonization.main.code.tiger.transformer.utils.IntegerSupport import \
    IntegerSupport


class IntegerTypeTransformer(IntegerSupport):
    def __init__(self):
        super().__init__(self)

    @staticmethod
    def standardizeIntegerType(value: str):
        return IntegerSupport().normalizeTrimAndRemoveString(value)
