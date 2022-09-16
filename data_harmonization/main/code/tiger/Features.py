import re
from typing import Tuple
from data_harmonization.main.code.tiger.features.Distance import Distance
import numpy as np

from data_harmonization.main.code.tiger.model.datamodel import RawEntity

class Features(RawEntity):

    def isEmpty(self, x: str):
        if (x == None) or (len(x) == 0):
            return True
        else:
            return False

    def removeWhiteSpaces(self, x:str):
        cleansed_list =  re.split("[-\\s]", x).filter(lambda s : not self.isEmpty(s))
        return "".join(cleansed_list)

    def engineerFeatures(self, name1: str, name2: str):
        if self.isEmpty(name1) or self.isEmpty(name2):
            return [1] * 4
        else:
            distance_obj = Distance()
            list(distance_obj.getLevenshteinDistance(self.removeWhiteSpaces(name1), self.removeWhiteSpaces(name2)),
            distance_obj.getCosineDistance(self.removeWhiteSpaces(name1), self.removeWhiteSpaces(name2)),
            distance_obj.getHammingDistance(self.removeWhiteSpaces(name1), self.removeWhiteSpaces(name2)),
            distance_obj.getJaroWinklerDistance(self.removeWhiteSpaces(name1), self.removeWhiteSpaces(name2)))

    # TODO: add capability to trim + lower case before applying these transformations

    def get(self, pairs: Tuple[RawEntity, RawEntity]) -> np.array:

        # TODO: name Features
        nFeatures = self.engineerFeatures(pairs[0].name, pairs[1].name) + self.engineerFeatures(pairs[0].address, pairs[1].address)
        + self.engineerFeatures(pairs[0].gender, pairs[1].gender)

        return nFeatures
