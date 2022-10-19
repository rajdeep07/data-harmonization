from typing import Optional

from data_harmonization.main.code.tiger.transformer.utils.FloatSupport import (
    FloatSupport,
)


class FloatTypeTransformer(FloatSupport):
    def __init__(self):
        super().__init__(self)

    @staticmethod
    def standardizeFloatType(value: str):
        return FloatSupport().normalizeTrimAndRemoveString(value)
