from curses.ascii import isdigit
import re
from typing import Any, Tuple
from data_harmonization.main.code.tiger.features.Distance import Distance
import numpy as np

from data_harmonization.main.code.tiger.model.datamodel import RawEntity

class Features():

    def isEmpty(self, x: Any) -> bool:
        if (x == None) or (x == ""):
            return True
        else:
            return False

    def removeWhiteSpaces(self, x:str):
        cleansed_list =  list(map(lambda val:val.strip().lower(), 
            filter(lambda s : not self.isEmpty(s), re.split("[-\\s]", x))))
        return "".join(cleansed_list)

    def engineerFeatures(self, entity1: str, entity2: str):
        if self.isEmpty(entity1) or self.isEmpty(entity2):
            return [1] * 4
        elif isinstance(entity1, str) and isinstance(entity2, str):
            distance_obj = Distance()
            return [distance_obj.getLevenshteinDistance(self.removeWhiteSpaces(entity1), self.removeWhiteSpaces(entity2)),
            distance_obj.getCosineDistance(self.removeWhiteSpaces(entity1), self.removeWhiteSpaces(entity2)),
            distance_obj.getHammingDistance(self.removeWhiteSpaces(entity1), self.removeWhiteSpaces(entity2)),
            distance_obj.getJaroWinklerDistance(self.removeWhiteSpaces(entity1), self.removeWhiteSpaces(entity2))]
        elif isinstance(entity1, int) or isinstance(entity1, int):
            return np.array([1]*4)
    # TODO: add capability to trim + lower case before applying these transformations

    def get(self, pairs: Tuple[dict, dict]) -> np.array:

        # TODO: name Features
        n_Features = np.array([])
        for key, value in pairs[0].items():
            n_Features = np.append(n_Features, np.array(self.engineerFeatures(pairs[0][key], pairs[1][key])))

        return n_Features


if __name__ == "__main__":
    pairs = ({
        'cluster_id': 38364, 'Name': '       presco', 'City': 'riverview', 'Zip': '335796903',
        'Address': '12502balmri        verviewrd', 'gender_field': None, 'source': 'flna', 'age': None}, 


        {'cluster_id': 38369, 'Name': 'riverview         sunoco (ib', 'City': 'riverview', 'Zip': '335796702', 
        'Address': '12302     balmriverviewrd', 'gender_field': None, 'source': 'flna', 'age': None
        }) 
    ft = Features().get(pairs=pairs)
    print(ft)
