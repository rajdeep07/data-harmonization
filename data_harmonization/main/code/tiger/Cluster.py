from tkinter.messagebox import NO
from data_harmonization.main.code.tiger.transformer.utils import StringSupport
from data_harmonization.main.code.tiger.transformer import CityTransformer, NameTransformer, \
    PostalAddressTransformer, StateTransformer
from data_harmonization.main.code.tiger.model import GeocodedAddress, PostalAddress, datamodel, SemiMergedProfile, RawProfile
from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
import pandas as pd
import numpy as np
from os import listdir
import os
import sys
from typing import Optional
import random
import itertools

class Cluster():

    filenames = listdir(os.getcwd() + "/data_harmonization/main/data/")
    csv_filenames = [filename for filename in filenames if filename.endswith(".csv")]
    rawProfiles = pd.DataFrame() # correct data structure here ?
    for csv_file in csv_filenames:
        rawProfiles = rawProfiles.append(pd.read_csv(os.getcwd() + f"/data_harmonization/main/data/{csv_file}"))

    # print(rawProfiles.head())
    # Flatten rawProfiles to fields which are only string / int

    rawProfilesWithTokens = rawProfiles.iloc[:200,:].apply(lambda r: Sanitizer().toRawEntity(r), axis=1)  #.filter(lambda p: p.id.isNotEmpty)

    # Flatten rawProfiles to fields which are only string / int

    # print(rawProfilesWithTokens.head(), rawProfilesWithTokens.iloc[:])

    def createflattenRawprofile(self) -> dict:
        flattenRawprofile = {}
        id = 0
        for raw_ent in self.rawProfilesWithTokens:
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
            flattenRawprofile[str(id)] = semiflattenRawprofile
            id = id+1
        return flattenRawprofile

    def filter_flat_map(self, f, arg:str):
        flatten_str = "".join(filter(lambda s:s!="", arg))
        return f(flatten_str)

    def createShingles(self, input: Optional[str]) -> Optional[list[str]]:
        def shingles(x:str) -> list[str]:
            i = x.lower() # TODO: remove extra unnecessary characters when creating shingles
            if len(i) > 5:
                return list(map(lambda j: i[j:j+5], range(0, len(i) - 5 + 1)))
            else:
                return list(i)


        # mapped_str = list(map(lambda i: i.split("[-\\s,]"), input))
        # flattend_str = "".join(filter(lambda s: s != "", mapped_str))
        return self.filter_flat_map(shingles, input)

    def createTokens(self, profile: dict, shingle_size: int) -> str:
        output = self.createShingles(profile.get('Name', "")) #+ self.createShingles(profile.get('Address'," ")) + self.createShingles(profile.get('City', " ")) + self.createShingles(profile.get('source'," "))
        # return output.flatten.filter(lambda x: (x is not None) and x.isNotEmpty)
        return output

    def get_minhash(self, tot_shingle, n_hashes, random_strings):
        minhash_row = []
        for i in range(n_hashes):
            minhash = sys.maxsize
            for shingle in tot_shingle:
                hash_candidate = abs(hash(shingle + random_strings[i]))
                if hash_candidate < minhash:
                    minhash = hash_candidate
            minhash_row.append(minhash)
        return minhash_row

    def get_band_hashes(self, minhash_row, band_size) -> list:
        band_hashes = []
        for i in range(len(minhash_row)):
            if i % band_size == 0:
                if i > 0:
                    band_hashes.append(band_hash)
                band_hash = 0
            band_hash += hash(minhash_row[i])
        return band_hashes

    def get_similar_docs(self, docs : dict, n_hashes : int =400, band_size : int = 7, shingle_size : int = 3, collectIndexes : bool =True):
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

if __name__ == '__main__':

    n_hashes = 200
    band_size = 7
    shingle_size = 3
    n_docs = 1000
    max_doc_length = 40
    n_similar_docs = 10
    random.seed(42)

    # docs = generate_random_docs(n_docs, max_doc_length, n_similar_docs)
    clus = Cluster()
    docs = clus.createflattenRawprofile()
    similar_docs = Cluster().get_similar_docs(docs, n_hashes, band_size, shingle_size, collectIndexes=False)
    print(similar_docs)

    r = float(n_hashes/band_size)
    similarity = (1/r)**(1/float(band_size))

    print("similarity: %f" % similarity)
    print("# Similar Pairs: %d" % len(similar_docs))

    if len(similar_docs) == n_similar_docs:
        print("Test Passed: All similar pairs found.")
    else:
        print("Test Failed.")