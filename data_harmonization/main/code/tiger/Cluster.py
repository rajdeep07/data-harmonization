import itertools
from multiprocessing.spawn import prepare
import os
import random
import re
import sys
from os import listdir
from typing import Any, Optional

import numpy as np
import pandas as pd

from data_harmonization.main.code.tiger.model.datamodel import *
from data_harmonization.main.code.tiger.model.GeocodedAddress import GeocodedAddress
from data_harmonization.main.code.tiger.model.PostalAddress import PostalAddress
from data_harmonization.main.code.tiger.model.SemiMergedProfile import SemiMergedProfile
from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
from data_harmonization.main.code.tiger.transformer import (
    CityTransformer,
    NameTransformer,
    PostalAddressTransformer,
    StateTransformer,
)
from data_harmonization.main.code.tiger.transformer.utils import StringSupport


class Cluster:
    """create cluster pairs using minLSH alogorithm"""

    def __init__(
        self,
        n_hashes: int = 4000,
        band_size: int = 5,
        shingle_size: int = 5,
        collect_indexes: bool = True,
        n_docs: Optional[int] = 1000,
    ):
        self.n_hashes = n_hashes
        self.band_size = band_size
        self.shingle_size = shingle_size
        self.collect_indexes = collect_indexes
        self.n_docs = n_docs
        self.flat_raw_profile = {}

    def prepare_data(self, data: Optional[pd.DataFrame] = None) -> dict:
        """Flatten raw_profiles to fields which are only string / int / float"""
        self.raw_profiles = data
        if not data:
            filenames = listdir(os.getcwd() + "/data_harmonization/main/data/")
            csv_filenames = [
                filename for filename in filenames if filename.endswith(".csv")
            ]
            raw_profiles = pd.DataFrame()
            for csv_file in csv_filenames:
                self.raw_profiles = raw_profiles.append(
                    pd.read_csv(
                        os.getcwd() + f"/data_harmonization/main/data/{csv_file}"
                    )
                )

        raw_profiles_with_tokens = self.raw_profiles.apply(
            lambda r: Sanitizer().toRawEntity(r), axis=1
        )  # .filter(lambda p: p.id._is_not_empty)
        id = 0
        for raw_ent in raw_profiles_with_tokens.sample(self.n_docs):
            semiflatten_raw_profile = {}
            raw_dict = raw_ent.__dict__
            for k, v in raw_dict.items():
                if not isinstance(v, (int, str, float)) and v:
                    for k1, v1 in v.__dict__.items():
                        semiflatten_raw_profile[k1] = v1
                else:
                    semiflatten_raw_profile[k] = v
            self.flat_raw_profile[str(id)] = semiflatten_raw_profile
            id = id + 1
        return self.flat_raw_profile

    def _flatten_list(self, l: list) -> list:
        return [
            item if isinstance(sublist, list) else sublist
            for sublist in l
            for item in sublist
        ]

    def _is_not_empty(self, input: Optional[Any] = None) -> bool:
        if input is None:
            return False
        elif isinstance(input, str):
            return bool(input.strip())

    # Step 1 : Create shingles
    def _create_shingles(
        self, input: Optional[str], shingle_size
    ) -> Optional[list[str]]:
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
                    list(filter(self._is_not_empty, re.split("[-\s\\\\,]s*", input))),
                )
            )
        )

    # Step 2: Tokenization
    def _create_tokens(self, profile: dict, shingle_size: int) -> str:
        output = []
        for v in profile.values():
            if isinstance(v, str):
                output.extend(self._create_shingles(v, shingle_size))
        return output

    # Step 3: Hash
    def _get_minhash(self, tot_shingle, n_hashes, random_strings) -> list:
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
    def _get_band_hashes(self, minhash_row, band_size) -> list:
        band_hashes = []
        for i in range(len(minhash_row)):
            if i % band_size == 0:
                if i > 0:
                    band_hashes.append(band_hash)
                band_hash = 0
            band_hash += hash(minhash_row[i])
        return band_hashes

    # Similar documents : LSH
    def do_blocking(self, docs: dict):
        hash_bands = {}
        random_strings = [str(random.random()) for _ in range(self.n_hashes)]
        docNum = 0
        for doc in docs.values():
            shingles = self._create_tokens(doc, self.shingle_size)
            minhash_row = self._get_minhash(shingles, self.n_hashes, random_strings)
            band_hashes = self._get_band_hashes(minhash_row, self.band_size)

            docMember = docNum if self.collect_indexes else doc
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
                        similar_docs.append(pair)
        return similar_docs

    # def fit(self, n_docs, data: Optional[pd.DataFrame] = None):
    #     self.prepare_data(n_docs, data)
    #     return self

    # def transform(
    #     self,
    #     n_hashes: int = 4000,
    #     band_size: int = 5,
    #     shingle_size: int = 5,
    #     collect_indexes: bool = True,
    # ):
    #     return self.do_blocking(
    #         docs=self.flat_raw_profile,
    #         n_hashes=n_hashes,
    #         band_size=band_size,
    #         shingle_size=shingle_size,
    #         collect_indexes=collect_indexes,
    #     )

    # def fit_transform(
    #     self,
    #     n_docs,
    #     data: Optional[pd.DataFrame] = None,
    #     n_hashes=4000,
    #     band_size=5,
    #     shingle_size=5,
    #     collect_indexes=True,
    # ):
    #     self.fit(data=data, n_docs=n_docs)
    #     return self.transform(
    #         n_hashes=n_hashes,
    #         band_size=band_size,
    #         shingle_size=shingle_size,
    #         collect_indexes=collect_indexes,
    #     )


if __name__ == "__main__":

    n_hashes = 200
    band_size = 5
    shingle_size = 5
    n_docs = 300
    max_doc_length = 400
    n_similar_docs = 10
    random.seed(42)

    clus = Cluster(
        n_hashes=n_hashes,
        band_size=band_size,
        shingle_size=shingle_size,
        collect_indexes=False,
        n_docs=n_docs,
    )
    # similar_docs = clus.fit_transform(
    #     n_docs,
    #     n_hashes=n_hashes,
    #     band_size=band_size,
    #     shingle_size=shingle_size,
    #     collect_indexes=False,
    # )
    prepared_data = clus.prepare_data()
    similar_docs = clus.do_blocking(prepared_data)
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
    df.to_csv(os.getcwd() + "/similiar.csv")

    r = float(n_hashes / band_size)
    similarity = (1 / r) ** (1 / float(band_size))

    print("similarity: %f" % similarity)
    print("# Similar Pairs: %d" % len(similar_docs))

    if len(similar_docs) == n_similar_docs:
        print("Test Passed: All similar pairs found.")
    else:
        print("Test Failed.")
