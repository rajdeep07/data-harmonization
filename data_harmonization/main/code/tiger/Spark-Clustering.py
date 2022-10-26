import itertools
import os
import random
import re
import sys
from os import listdir
from typing import Any, Optional

import numpy as np
import pandas as pd

from data_harmonization.main.code.tiger.model.SemiMergedProfile import \
    SemiMergedProfile
from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
from data_harmonization.main.code.tiger.transformer import (
    CityTransformer, NameTransformer, PostalAddressTransformer,
    StateTransformer)
from data_harmonization.main.code.tiger.transformer.utils import StringSupport


class Cluster:
    """create cluster pairs using minLSH alogorithm"""

    # TODO: create wrapper for reading datasets
    # filenames = listdir(os.getcwd() + "/data_harmonization/main/data/")
    # csv_filenames = [filename for filename in filenames if filename.endswith(".csv")]
    # rawProfiles = pd.DataFrame() # correct data structure here ?
    # for csv_file in csv_filenames:
    #     rawProfiles = rawProfiles.append(pd.read_csv(os.getcwd() + f"/data_harmonization/main/data/{csv_file}"))
    # print(rawProfiles.head())
    # Flatten rawProfiles to fields which are only string / int

    # Cleansing of the data set
    flattenRawprofile = {}

    # Flatten rawProfiles to fields which are only string / int / float
    def createflattenRawprofile(
        self, data: Optional[pd.DataFrame] = None, n_docs: Optional[int] = 1000
    ) -> dict:
        self.rawProfiles = data
        if not data:
            filenames = listdir(os.getcwd() + "/data_harmonization/main/data/")
            csv_filenames = [
                filename for filename in filenames if filename.endswith(".csv")
            ]
            rawProfiles = pd.DataFrame()  # correct data structure here ?
            for csv_file in csv_filenames:
                self.rawProfiles = rawProfiles.append(
                    pd.read_csv(
                        os.getcwd() + f"/data_harmonization/main/data/{csv_file}"
                    )
                )

        rawProfilesWithTokens = self.rawProfiles.apply(
            lambda r: Sanitizer().toRawEntity(r), axis=1
        )  # .filter(lambda p: p.id._isNotEmpty)
        id = 0
        for raw_ent in rawProfilesWithTokens.sample(n_docs):
            semiflattenRawprofile = {}
            # raw_ent.__dict__.items()
            raw_dict = raw_ent.__dict__
            # semiflattenRawprofile = {k:v for k, v in raw_dict.items() if k != "cluster_id"}
            for k, v in raw_dict.items():
                if not isinstance(v, (int, str, float)) and v:
                    for k1, v1 in v.__dict__.items():
                        semiflattenRawprofile[k1] = v1
                else:
                    semiflattenRawprofile[k] = v
            # flattenRawprofile[raw_ent["cluster_id"]] = {k1:v1 for k, v in semiflattenRawprofile.items() if not isinstance(v, (int, str, float) and v) for k1, v1 in v.__dict__.items()}
            self.flattenRawprofile[str(id)] = semiflattenRawprofile
            id = id + 1
        return self.flattenRawprofile

    def _flatten_list(self, l: list) -> list:
        return [
            item if isinstance(sublist, list) else sublist
            for sublist in l
            for item in sublist
        ]
        # return [item for sublist in l if isinstance(sublist, list) for item in sublist else sublist]

    def _isNotEmpty(self, input: Optional[Any] = None) -> bool:
        if input is None:
            return False
        elif isinstance(input, str):
            return bool(input.strip())

    # Step 1 : Create shingles
    """def createShingles(self, input: Optional[str]) -> Optional[list[str]]:
        def shingles(x:str) -> list[str]:
            i = x.lower() # TODO: remove extra unnecessary characters when creating shingles
            if len(i) > 5:
                a = list(map(lambda j: i[j:j+5], range(0, len(i) - 5 + 1)))
                # return list(map(lambda j: i[j:j+5], range(0, len(i) - 5 + 1)))
                return a
            else:
                return list(i)


        # mapped_str = list(map(lambda i: i.split("[-\\s,]"), input))
        # flattend_str = "".join(filter(lambda s: s != "", mapped_str))
        return self.filter_flat_map(shingles, input)"""

    # Create Shingles
    def createShingles(self, input: Optional[str], shingle_size) -> Optional[list[str]]:
        def shingles(x: str) -> list[str]:
            i = (
                x.lower()
            )  # TODO: remove extra unnecessary characters when creating shingles
            if len(i) > shingle_size:
                return list(
                    map(
                        lambda j: i[j : j + shingle_size],
                        range(0, len(i) - shingle_size + 1),
                    )
                )
            else:
                return [i]

        return self._flatten_list(
            list(
                map(
                    shingles,
                    list(filter(self._isNotEmpty, re.split("[-\s\\\\,]s*", input))),
                )
            )
        )

    # Step 2: Tokenization
    def createTokens(self, profile: dict, shingle_size: int) -> str:
        # output = self.createShingles(profile.get('Name', "")) + self.createShingles(profile.get('City'," ")) + self.createShingles(profile.get('Address', " ")) #+ self.createShingles(profile.get('source'," "))
        output = []
        for v in profile.values():
            if isinstance(v, str):
                output.extend(self.createShingles(v, shingle_size))
        # return output.flatten.filter(lambda x: (x is not None) and x._isNotEmpty)
        return output

    # Step 3: Hash
    def get_minhash(self, tot_shingle, n_hashes, random_strings) -> list:
        minhash_row = []
        for i in range(n_hashes):
            minhash = sys.maxsize
            for shingle in tot_shingle:
                hash_candidate = abs(hash(shingle + random_strings[i]))
                if hash_candidate < minhash:
                    minhash = hash_candidate
            minhash_row.append(minhash)
        return minhash_row

    # LSH ==> MinLSH [More reading]
    def get_band_hashes(self, minhash_row, band_size) -> list:
        band_hashes = []
        for i in range(len(minhash_row)):
            if i % band_size == 0:
                if i > 0:
                    band_hashes.append(band_hash)
                band_hash = 0
            band_hash += hash(minhash_row[i])
        return band_hashes

    # Similar documents : LSH
    def get_similar_docs(
        self,
        docs: dict,
        n_hashes: int = 4000,
        band_size: int = 5,
        shingle_size: int = 5,
        collectIndexes: bool = True,
    ):
        hash_bands = {}
        random_strings = [str(random.random()) for _ in range(n_hashes)]
        docNum = 0
        for doc in docs.values():
            shingles = self.createTokens(doc, shingle_size)
            minhash_row = self.get_minhash(shingles, n_hashes, random_strings)
            band_hashes = self.get_band_hashes(minhash_row, band_size)

            docMember = docNum if collectIndexes else doc
            for i in range(len(band_hashes)):
                if i not in hash_bands:
                    hash_bands[i] = {}
                if band_hashes[i] not in hash_bands[i]:
                    hash_bands[i][band_hashes[i]] = [docMember]
                else:
                    hash_bands[i][band_hashes[i]].append(docMember)
            docNum += 1

        similar_docs = []
        for i in hash_bands:
            for hash_num in hash_bands[i]:
                if len(hash_bands[i][hash_num]) > 1:
                    for pair in itertools.combinations(hash_bands[i][hash_num], r=2):
                        # print(pair)
                        similar_docs.append(pair)
        return similar_docs

    def fit(self, n_docs, data: Optional[pd.DataFrame] = None):
        self.createflattenRawprofile(data, n_docs)
        return self

    def transform(
        self,
        n_hashes: int = 4000,
        band_size: int = 5,
        shingle_size: int = 5,
        collectIndexes: bool = True,
    ):
        return self.get_similar_docs(
            docs=self.flattenRawprofile,
            n_hashes=n_hashes,
            band_size=band_size,
            shingle_size=shingle_size,
            collectIndexes=collectIndexes,
        )

    def fit_transform(
        self,
        n_docs,
        data: Optional[pd.DataFrame] = None,
        n_hashes=4000,
        band_size=5,
        shingle_size=5,
        collectIndexes=True,
    ):
        self.fit(data=data, n_docs=n_docs)
        return self.transform(
            n_hashes=n_hashes,
            band_size=band_size,
            shingle_size=shingle_size,
            collectIndexes=collectIndexes,
        )


if __name__ == "__main__":

    n_hashes = 200
    band_size = 5
    shingle_size = 5
    n_docs = 300
    max_doc_length = 400
    n_similar_docs = 10
    random.seed(42)

    # docs = generate_random_docs(n_docs, max_doc_length, n_similar_docs)
    clus = Cluster()
    # clus.fit(n_docs=n_docs)
    # similar_docs = clus.transform(n_hashes, band_size, shingle_size, collectIndexes=False)
    similar_docs = clus.fit_transform(
        n_docs,
        n_hashes=n_hashes,
        band_size=band_size,
        shingle_size=shingle_size,
        collectIndexes=False,
    )
    print(similar_docs[0])
    df_dict = {}
    for pair1, pair2 in similar_docs:
        for k, v in pair1.items():
            key_var = f"{k}" + "_pair1"
            if not df_dict.get(key_var, None):
                df_dict[key_var] = []
                df_dict[key_var].append(v)
            else:
                df_dict[key_var].append(v)

        for k, v in pair2.items():
            key_var = f"{k}" + "_pair2"
            if not df_dict.get(key_var, None):
                df_dict[key_var] = []
                df_dict[key_var].append(v)
            else:
                df_dict[key_var].append(v)
    df = pd.DataFrame(df_dict)
    # df.drop_duplicates(inplace=True)
    df.to_csv(os.getcwd() + "/similiar.csv")

    r = float(n_hashes / band_size)
    similarity = (1 / r) ** (1 / float(band_size))

    print("similarity: %f" % similarity)
    print("# Similar Pairs: %d" % len(similar_docs))

    if len(similar_docs) == n_similar_docs:
        print("Test Passed: All similar pairs found.")
    else:
        print("Test Failed.")


# _pbna.csv ==> class _pbna ==> add UUID column ==> persist MySQL pbna (AS-IS)
# _flna.csv == > class _flna == > add UUID column == > persist MySQL flna (AS-IS)

# Assumption : Name correction [Either User do it or we have to do before leveraging this platform]
# DropBox [Name, Address, ZipCode, ...]

# ==> RawEntity (default : common attributes of all pbna & flna excluding UUID / user_select_deduplicated_column) == >
# read appropriate columns from pbna & flna mysql tables
# def toRawEntity(tables): RawEntity:

# cleansing(RawEntity) ==> persist MySQL ()
