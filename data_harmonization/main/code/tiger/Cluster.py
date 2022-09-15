from data_harmonization.main.code.tiger.transformer.utils import StringSupport
from data_harmonization.main.code.tiger.transformer import CityTransformer, NameTransformer, \
    PostalAddressTransformer, StateTransformer
from data_harmonization.main.code.tiger.model import GeocodedAddress, PostalAddress, datamodel, SemiMergedProfile
from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
import pandas as pd
import numpy as np
from os import listdir
import os

class Cluster(datamodel, SemiMergedProfile):

    filenames = listdir(os.getcwd() + "/data_harmonization/main/data/")
    csv_filenames = [filename for filename in filenames if filename.endswith(".csv")]
    print(csv_filenames)
    rawProfiles = pd.DataFrame() # correct data structure here ?
    for csv_file in csv_filenames:
        rawProfiles = rawProfiles.append(pd.read_csv(os.getcwd() + f"/data_harmonization/main/data/{csv_file}"))

    print(rawProfiles.head())
    # Flatten rawProfiles to fields which are only string / int

    rawProfilesWithTokens = rawProfiles.apply(lambda r: Sanitizer().toRawEntity(r), axis=1)  #.filter(lambda p: p.id.isNotEmpty)

    # Flatten rawProfiles to fields which are only string / int

    print(rawProfilesWithTokens)

    def createShingles(self, input: Optional[str]) -> Optional[Seq[str]]:
        def shingles(x:str) -> Seq[str]:
            i = x.lower # TODO: remove extra unnecessary characters when creating shingles
            if i.length > 5:
                Range(0, i.length - 5 + 1).map(lambda j: i[j:j+5])
            else:
                Seq(i)

        input.map(lambda i: i.split("[-\\s,]").filter(lambda s: s.isNotEmpty).flatmap(shingles).Seq)

    def createTokens(self, profile: RawProfile) -> str:
        output = createShingles(profile.name) + createShingles(profile.city) + createShingles(profile.address) + createShingles(state)
        output.flatten.filter(lambda x: (x is not None) and x.isNotEmpty)

    def get_minhash(self, tot_shingle, n_hashes, random_strings):
        minhash_row = []
        for i in range(n_hashes):
            minhash = sys.maxint
            for shingle in tot_shingle:
                hash_candidate = abs(hash(shingle + random_strings[i]))
                if hash_candidate < minhash:
                    minhash = hash_candidate
            minhash_row.append(minhash)
        return minhash_row

    def get_band_hashes(self, minhash_row, band_size):
        band_hashes = []
        for i in range(len(minhash_row)):
            if i % band_size == 0:
                if i > 0:
                    band_hashes.append(band_hash)
                band_hash = 0
            band_hash += hash(minhash_row[i])
        return band_hashes

    def get_similar_docs(self, docs, n_hashes=400, band_size=7, shingle_size=3, collectIndexes=True):
        hash_bands = {}
        random_strings = [str(random()) for _ in range(n_hashes)]
        docNum = 0
        for doc in docs:
            shingles = createTokens(RawProfile)
            minhash_row = get_minhash(shingles, n_hashes, random_strings)
            band_hashes = get_band_hashes(minhash_row, band_size)

            docMember = docNum if collectIndexes else doc
            for i in range(len(band_hashes)):
                if i not in hash_bands:
                    hash_bands[i] = {}
                if band_hashes[i] not in hash_bands[i]:
                    hash_bands[i][band_hashes[i]] = [docMember]
                else:
                    hash_bands[i][band_hashes[i]].append(docMember)
            docNum += 1

        similar_docs = set()
        for i in hash_bands:
            for hash_num in hash_bands[i]:
                if len(hash_bands[i][hash_num]) > 1:
                    for pair in itertools.combinations(hash_bands[i][hash_num], r=2):
                        similar_docs.add(pair)

        return similar_docs

if __name__ == '__main__':

    n_hashes = 200
    band_size = 7
    shingle_size = 3
    n_docs = 1000
    max_doc_length = 40
    n_similar_docs = 10
    seed(42)

    docs = generate_random_docs(n_docs, max_doc_length, n_similar_docs)
    similar_docs = get_similar_docs(docs, n_hashes, band_size, shingle_size, collectIndexes=False)
    print(similar_docs)

    r = float(n_hashes/band_size)
    similarity = (1/r)**(1/float(band_size))

    print("similarity: %f" % similarity)
    print("# Similar Pairs: %d" % len(similar_docs))

    if len(similar_docs) == n_similar_docs:
        print("Test Passed: All similar pairs found.")
    else:
        print("Test Failed.")