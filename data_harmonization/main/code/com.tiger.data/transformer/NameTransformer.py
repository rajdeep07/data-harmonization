from data_harmonization.main.code.com.tiger.data.transformer.utils import StringSupport
from data_harmonization.main.code.com.tiger.data.model import RawProfile
from typing import Optional


class NameTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    def standardizeName(self, value: Option[str]):
        value.map(lambda x: x.normalizeString())\
            .map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveNumbers)\
            .map(lambda x: x.normalizeTrimAndLowerCaseStringAndRemoveSpecialCharacters)\
            .filter(lambda x: x.nonEmpty)

    def standardizeAndBetweenNames(self, value: str):
        list_string = value.replace("[&|\\+]", " and ").replace(" +", " ").split("(?<= )")
        intermediate_string =  ' '.join(list_string).replace("(and )+", "and ").replace(" and\\s$", "")
        return " ".join(intermediate_string.split(" and ").sort(lambda x: x.lower()))


