from typing import Optional

from data_harmonization.main.code.tiger.model.datamodel import *
from data_harmonization.main.code.tiger.transformer.utils.StringSupport import *


class NameTransformer(StringSupport):
    # Config driven for advanced profile synthesis
    def __init__(self):
        super().__init__(self)

    @staticmethod
    def _get_attr_list(obj, should_print=False):
        items = obj.__dict__.items()
        if should_print:
            [
                print(f"attribute: {k}  value: {v}")
                for k, v in items
                if k in ("__annotations__")
            ]
        return (value for item, value in items if item in ("__annotations__"))

    @staticmethod
    def standardizeName(value: Optional[str]):
        stringSupportObj = StringSupport()
        sub_attr_list = list(NameTransformer._get_attr_list(Name))[0]
        for sub_attr in sub_attr_list.keys():
            setattr(
                value,
                sub_attr,
                stringSupportObj.normalizeTrimAndLowerCaseStringAndRemoveNumbers(
                    getattr(value, sub_attr)
                ),
            )

        return value
        # return value.map(lambda x: x.normalizeString())\
        #     .map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveNumbers)\
        #     .map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)\
        #     .filter(lambda x: x.nonEmpty)

    def standardizeAndBetweenNames(self, value: str):
        list_string = (
            value.replace("[&|\\+]", " and ").replace(" +", " ").split("(?<= )")
        )
        intermediate_string = (
            " ".join(list_string).replace("(and )+", "and ").replace(" and\\s$", "")
        )
        return " ".join(intermediate_string.split(" and ").sort(lambda x: x.lower()))
