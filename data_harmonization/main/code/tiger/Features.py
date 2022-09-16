import re
from typing import Tuple
from data_harmonization.main.code.tiger.features.Distance import Distance
import numpy as np

from data_harmonization.main.code.tiger.model.datamodel import RawEntity

class Features():

    def isEmpty(self, x: str):
        if (x == None) or (len(x) == 0):
            return True
        else:
            return False

    def removeWhiteSpaces(self, x:str):
        cleansed_list =  filter(lambda s : not self.isEmpty(s), re.split("[-\\s]", x))
        return "".join(cleansed_list)

    def cleanEntity(self, name:str):
        pass

    def engineerFeatures(self, name1: str, name2: str):
        if self.isEmpty(name1) or self.isEmpty(name2):
            return [1] * 4
        else:
            distance_obj = Distance()
            return list(distance_obj.getLevenshteinDistance(self.removeWhiteSpaces(name1), self.removeWhiteSpaces(name2)),
            distance_obj.getCosineDistance(self.removeWhiteSpaces(name1), self.removeWhiteSpaces(name2)),
            distance_obj.getHammingDistance(self.removeWhiteSpaces(name1), self.removeWhiteSpaces(name2)),
            distance_obj.getJaroWinklerDistance(self.removeWhiteSpaces(name1), self.removeWhiteSpaces(name2)))

    # TODO: add capability to trim + lower case before applying these transformations

    def get(self, pairs: Tuple[dict, dict]) -> np.array:

        # TODO: name Features
        n_Features = np.array([])
        for pair_dict in pairs:
            for key, value in pair_dict.items():
                if isinstance(value, str):
                    n_Features = np.append(n_Features, np.array(self.engineerFeatures(pairs[0][key], pairs[1][key])), axis=0)

        return n_Features


if __name__ == "__main__":
    pairs = ({
        'cluster_id': 38364, 'Name': 'presco', 'City': 'riverview', 'Zip': '335796903',
        'Address': '12502balmriverviewrd', 'gender_field': None, 'source': 'flna', 'age': None}, 
        {'cluster_id': 38369, 'Name': 'riverview sunoco (ib', 'City': 'riverview', 'Zip': '335796702', 
        'Address': '12302balmriverviewrd', 'gender_field': None, 'source': 'flna', 'age': None
        }) 
    ft = Features().get(pairs=pairs)
    print(ft)
