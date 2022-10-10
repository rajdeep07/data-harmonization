import itertools
import os
import random
import re
import sys
from os import listdir
from typing import Any, Optional

import numpy as np
import pandas as pd
from pyspark.sql import Row

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
from data_harmonization.main.code.tiger.database.MySQL import MySQL
import data_harmonization.main.resources.config as config_
from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass


class Cluster:
    """create cluster pairs using minLSH alogorithm"""

    # TODO: create wrapper for reading datasets

    flattenRawprofile = {}

    def get_data_from_db(self, table_name):
        # mysql = MySQL(
        #     config_.mysqlLocalHost,
        #     config_.APP_NAME,
        #     config_.mysqlUser,
        #     config_.mysqlPassword,
        # )
        # mysql.mycursor.execute(f"SELECT * FROM {table_name};")
        # return mysql.mycursor.fetchall()

        spark = SparkClass()
        return spark.read_from_database_to_dataframe(table_name)

    def _to_entity(self, df, cls):
        def _map_to_raw_entity(row):
            # print(row)
            obj = RawEntity()
            # fields = row.__fields__
            # return obj
            attributes = obj.__dict__.items()
            for attr, val in attributes:
                if attribute_value := row.__getitem__(attr):
                    setattr(obj, attr, attribute_value)

            return obj

        raw_profiles = (
            df.rdd.map(lambda r: _map_to_raw_entity(r))  # self._map_to_object(r, cls)
            .toDF()
            .na.drop(how="all")
        )
        raw_profiles.show()
        return raw_profiles

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
            lambda r: Sanitizer().toRawEntity(r, gen_id=True, clean_data=True), axis=1
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
    def createTokens(self, profile: Row, shingle_size: int) -> str:
        # def createTokens(self, profile: dict, shingle_size: int) -> str:
        output = []
        for v in profile.asDict().values():
            # for v in profile.values():
            if isinstance(v, str):
                output.extend(self.createShingles(v, shingle_size))
        # return output.flatten.filter(lambda x: (x is not None) and x._isNotEmpty)
        # return output
        row = Row(output)
        return row

    # Step 3: Hash
    def get_minhash(
        self, tot_shingle: Row, n_hashes: int, random_strings: list
    ) -> list:
        minhash_row = []
        for i in range(n_hashes):
            minhash = sys.maxsize
            a = tot_shingle.__getitem__("_1")
            for shingle in tot_shingle.__getitem__("_1"):
                hash_candidate = abs(hash(shingle + random_strings[i]))
                if hash_candidate < minhash:
                    minhash = hash_candidate
            minhash_row.append(minhash)
        return Row(minhash_row)

    # LSH ==> MinLSH [More reading]
    def get_band_hashes(self, minhash_row: Row, band_size: int) -> list:
        band_hashes = []
        minhash_list = list(minhash_row.__getitem__("_1"))
        # print("minhash size : ", str(len(minhash_list)))
        band_hash = 0
        for i in range(len(minhash_list)):
            if i % band_size == 0:
                if i > 0:
                    band_hashes.append(band_hash)
                band_hash = 0
            band_hash += hash(minhash_list[i])
        return Row(band_hashes)

    # Similar documents : LSH
    def get_similar_docs(
        self,
        docs: Row,
        n_hashes: int = 4000,
        band_size: int = 5,
        shingle_size: int = 5,
        collectIndexes: bool = True,
    ):
        hash_bands = {}
        random_strings = [str(random.random()) for _ in range(n_hashes)]
        docNum = 0
        # b = list(docs.__getitem__("_1"))
        # print(b)
        # print("band hashes size: ", str(len(b)))
        # for doc in docs.values():
        # for band_hashes in list(docs.__getitem__("_1")):
        # shingles = self.createTokens(doc, shingle_size)
        # minhash_row = self.get_minhash(shingles, n_hashes, random_strings)
        # band_hashes = self.get_band_hashes(minhash_row, band_size)

        # docMember = docNum if collectIndexes else band_hashes
        # for i in range(len(band_hashes)):
        #     if i not in hash_bands:
        #         hash_bands[i] = {}
        #     if band_hashes[i] not in hash_bands[i]:
        #         hash_bands[i][band_hashes[i]] = [docMember]
        #     else:
        #         hash_bands[i][band_hashes[i]].append(docMember)
        # docNum += 1
        band_hashes = list(docs.__getitem__("_1"))
        docMember = docNum if collectIndexes else band_hashes
        for i in range(len(band_hashes)):
            # for i in range(len(band_hashes)):
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
                if len(hash_bands[i][hash_num][0]) > 1:
                    for pair in itertools.combinations(hash_bands[i][hash_num][0], r=2):
                        # print("Pairs : ", pair)
                        similar_docs.append(pair)
        return Row(similar_docs)

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
    df = clus.get_data_from_db("Raw_Entity")
    raw_profiles = clus._to_entity(df, RawEntity)
    raw_profiles.show()
    raw_profiles_RE_class_tokens = raw_profiles.rdd.map(
        lambda r: clus.createTokens(r, shingle_size)
    ).toDF()
    raw_profiles_RE_class_tokens.show()
    print("Generate min hash")
    random_strings = [str(random.random()) for _ in range(n_hashes)]
    raw_profiles_minhash = raw_profiles_RE_class_tokens.rdd.map(
        lambda r: clus.get_minhash(r, n_hashes, random_strings)
    ).toDF()
    raw_profiles_band_hashes = raw_profiles_minhash.rdd.map(
        lambda r: clus.get_band_hashes(r, band_size)
    ).toDF()
    raw_profiles_band_hashes.show()
    a = raw_profiles_band_hashes.rdd.map(
        lambda r: clus.get_similar_docs(r, n_hashes, band_size, shingle_size, False)
    ).toDF()
    a.show()

    # similar_docs = clus.fit_transform(
    #     n_docs,
    #     n_hashes=n_hashes,
    #     band_size=band_size,
    #     shingle_size=shingle_size,
    #     collectIndexes=False,
    # )
    # print(similar_docs[0])
    # df_dict = {}
    # for pair1, pair2 in similar_docs:
    #     for k, v in pair1.items():
    #         key_var = f"{k}" + "_pair1"
    #         if not df_dict.get(key_var, None):
    #             df_dict[key_var] = []
    #             df_dict[key_var].append(v)
    #         else:
    #             df_dict[key_var].append(v)

    #     for k, v in pair2.items():
    #         key_var = f"{k}" + "_pair2"
    #         if not df_dict.get(key_var, None):
    #             df_dict[key_var] = []
    #             df_dict[key_var].append(v)
    #         else:
    #             df_dict[key_var].append(v)
    # df = pd.DataFrame(df_dict)
    # # df.drop_duplicates(inplace=True)
    # df.to_csv(os.getcwd() + "/similiar.csv")

    # r = float(n_hashes / band_size)
    # similarity = (1 / r) ** (1 / float(band_size))

    # print("similarity: %f" % similarity)
    # print("# Similar Pairs: %d" % len(similar_docs))

    # if len(similar_docs) == n_similar_docs:
    #     print("Test Passed: All similar pairs found.")
    # else:
    #     print("Test Failed.")
