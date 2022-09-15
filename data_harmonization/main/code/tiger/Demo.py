

import re
from typing import Any, Optional, Sequence


def createShingles(input: Optional[str] = None) -> Optional[Sequence[str]]:
        def shingles(x:str) -> Sequence[str]:
            shingle_list = []
            i = x.lower() # TODO: remove extra unnecessary characters when creating shingles
            if len(i) > 5:

                # range(0, len(i) - 5 + 1).map(lambda j: i[j:j+5])
                for j in range(0, len(i) - 5 + 1):
                    shingle_list.append(i[j:j+5])
            else:
                shingle_list.append(i)
                # Sequence(i)
            return shingle_list
        # input.map(lambda i: i.split("[-\\s,]").filter(lambda s: s.isNotEmpty).flatmap(shingles).Seq)

        print(list(filter(isNotEmpty, re.split("[-\s\\\\,]s*", input))))
        print(flatten_list(list(map(shingles, list(filter(isNotEmpty, re.split("[-\s\\\\,]s*", input)))))))
        #se = shingles(input)
        #print(se)

def isNotEmpty(input: Optional[Any]=None) -> bool:
    if input is None:
        return False
    elif isinstance(input, str):
        return bool(input.strip())

def flatten_list(l):
    return [item for sublist in l for item in sublist]

if __name__ == '__main__':

    createShingles("abcdefgh\\ijfsdfdsfd\ kldfscs mdewdwfregdfgbdfvn- opqrsetufdbvdfvfdvfvfsvv   df    dvsdvsdvsdvwxy, z")