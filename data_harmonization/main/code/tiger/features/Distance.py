import math
import re
from collections import Counter

import jaro
import Levenshtein
from metaphone import doublemetaphone


class Distance:
    def __init__(self):
        pass

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
            return Levenshtein.jaro_winkler(value1, value2)

    def getHammingDistance(self, value1: str, value2: str):
        return sum(c1 != c2 for c1, c2 in zip(value1, value2))

    def calcCosineDistance(self, value1: dict, value2: dict):
        intersection = set(value1.keys()) & set(value2.keys())
        numerator = sum([value1[x] * value2[x] for x in intersection])

        sum1 = sum([value1[x] ** 2 for x in value1])
        sum2 = sum([value2[x] ** 2 for x in value2])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)

        if not denominator:
            return 0.0
        else:
            return float(numerator) / denominator

        # if not denominator:
        #     return 0.0
        # else:
        #     return float(numerator) / denominator
        return 1

    def getCosineDistance(self, text1: str, text2: str):
        # TODO: use lowercase variables
        WORD = re.compile(r"\w+")
        word1 = Counter(WORD.findall(text1))
        word2 = Counter(WORD.findall(text2))
        cosine_dist = self.calcCosineDistance(word1, word2)
        return cosine_dist
