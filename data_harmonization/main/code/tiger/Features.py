from data_harmonization.main.code.tiger.features.Distance import Distance
import numpy as np

class Features(RawEntity):

    def isEmpty(self, x: str):
        if (x == Null) or (len(x) == 0):
            return True
        else:
            return False

    def removeWhiteSpaces(self, x:str):
        cleansed_list =  x.split("[-\\s]").filter(lambda s : not s.isEmpty())
        return "".join(cleansed_list)

    def engineerFeatures(self, name1: str, name2: str):
        if name1.isEmpty() or name2.isEmpty():
            return [1] * 4
        else:
            list(Distance.getLevenshteinDistance(removeWhiteSpaces(name1), removeWhiteSpaces(name2)),
            Distance.getCosineDistance(removeWhiteSpaces(name1), removeWhiteSpaces(name2)),
            Distance.getHammingDistance(removeWhiteSpaces(name1), removeWhiteSpaces(name2)),
            Distance.getJaroWinklerDistance(removeWhiteSpaces(name1), removeWhiteSpaces(name2)))

    # TODO: add capability to trim + lower case before applying these transformations

    def get(self, pairs: (RawEntity, RawEntity)) -> np.array:

        # TODO: name Features
        nFeatures = engineerFeatures(pairs[0].name, pairs[1].name) + engineerFeatures(pairs[0].address, pairs[1].address)
        + engineerFeatures(pairs[0].gender, pairs[1].gender)

        return nFeatures
