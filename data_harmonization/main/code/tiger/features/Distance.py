import math
from metaphone import doublemetaphone
import Levenshtein
import jaro


class Distance:

    def isEmpty(self, x: str):
        return x == None or len(x) == 0

    def getLevenshteinDistance(self, value1: str, value2: str) -> float:
        if self.isEmpty(value1) or self.isEmpty(value2):
            return 1.0
        else:
            return Levenshtein.distance(value1, value2)

    def getJaroWinklerDistance(self, value1: str, value2: str) -> float:
        if self.isEmpty(value1) or self.isEmpty(value2):
            return 1.0
        else:
            return jaro.jaro_winkler_distance(value1, value2)

    def getHammingDistance(self, value1: str, value2: str):
        return sum(c1 != c2 for c1, c2 in zip(value1, value2))

    def getCosineDistance(self, value1: str, value2: str):
        intersection = set(value1.keys()) & set(value2.keys())
        numerator = sum([value1[x] * value2[x] for x in intersection])

        sum1 = sum([value1[x] ** 2 for x in value1.keys()])
        sum2 = sum([value2[x] ** 2 for x in value2.keys()])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)

        if not denominator:
            return 0.0
        else:
            return float(numerator) / denominator
