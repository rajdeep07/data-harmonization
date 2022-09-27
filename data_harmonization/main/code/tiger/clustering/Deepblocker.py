from pathlib import Path
import re
from typing import Any, Optional
from data_harmonization.main.code.tiger.transformer.utils import StringSupport
from data_harmonization.main.code.tiger.transformer import CityTransformer, NameTransformer, \
    PostalAddressTransformer, StateTransformer
from data_harmonization.main.code.tiger.model.GeocodedAddress import GeocodedAddress
from data_harmonization.main.code.tiger.model.PostalAddress import PostalAddress
from data_harmonization.main.code.tiger.model.SemiMergedProfile import SemiMergedProfile
from data_harmonization.main.code.tiger.model.datamodel import *
from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
import pandas as pd
import numpy as np
from os import listdir
import os
import sys
import random
import itertools

from deep_blocker import DeepBlocker
from tuple_embedding_models import  AutoEncoderTupleEmbedding, CTTTupleEmbedding, HybridTupleEmbedding
from vector_pairing_models import ExactTopKVectorPairing
import blocking_utils

class Cluster_Deepblocker():
    """create cluster pairs using DeepBlocker"""
    # TODO: create wrapper for reading datasets
    current_dir = os.path.dirname(os.path.realpath(__file__))
    target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-3])
    filenames = listdir(target_dir + "/data/")

    # filenames = listdir(os.getcwd() + "/data_harmonization/main/data/")
    csv_filenames = [filename for filename in filenames if filename.endswith(".csv")]
    # rawProfiles = pd.DataFrame() # correct data structure here ?
    rawProfiles = []
    rawProfilesWithTokens = []
    i = 0
    for csv_file in csv_filenames:
        rawProfiles.append(pd.read_csv(os.getcwd() + f"/data_harmonization/main/data/{csv_file}"))
        # Cleansing of the data set
        rawProfilesWithTokens.append(rawProfiles[i].apply(lambda r: Sanitizer().toRawEntity(r), axis=1))  #.filter(lambda p: p.id.isNotEmpty)
        i = i + 1

    # Flatten rawProfiles to fields which are only string / int
    def createflattenRawprofile(self, n_docs: Optional[int]) -> dict:
        flattenRawprofile = []
        id = 0
        flattenRawprofileDF = []
        for rawProfilesWithToken in self.rawProfilesWithTokens:
            for raw_ent in rawProfilesWithToken.sample(n_docs):
                semiflattenRawprofile = {}
                raw_dict = raw_ent.__dict__
                for k, v in raw_dict.items():
                    if not isinstance(v, (int, str, float)) and v:
                        for k1, v1 in v.__dict__.items():
                            semiflattenRawprofile[k1] = v1
                    else:
                        semiflattenRawprofile[k] = v
                flattenRawprofile.append(semiflattenRawprofile)
            flattenRawprofileDF.append(pd.DataFrame.from_dict(flattenRawprofile))
        return flattenRawprofileDF

    def isNotEmpty(self, input: Optional[Any]=None) -> bool:
        if input is None:
            return False
        elif isinstance(input, str):
            return bool(input.strip())

    def do_blocking(self, left_df, right_df, cols_to_block, tuple_embedding_model, vector_pairing_model):
        # folder_root = Path(folder_root)
        # left_df = pd.read_csv(folder_root / left_table_fname)
        # right_df = pd.read_csv(folder_root / right_table_fname)

        db = DeepBlocker(tuple_embedding_model, vector_pairing_model)
        candidate_set_df = db.block_datasets(left_df, right_df, cols_to_block)

        # left_matched_df = left_df.loc[candidate_set_df["ltable_id"]]
        # left_matched_df["ltable_id"] = candidate_set_df["ltable_id"]
        # right_matched_df = right_df.loc[candidate_set_df["rtable_id"]]
        # right_matched_df["rtable_id"] = candidate_set_df["rtable_id"]
        # similar_data = pd.concat([left_matched_df, right_matched_df], axis="columns")
        candidate_set_df.to_csv(self.target_dir + "/main/deepblocker_matching.csv", mode='w+')
        csv_file = "benchmark.csv"
        golden_df = pd.read_csv(self.target_dir + f"/main/data/{csv_file}") # pd.read_csv(Path(folder_root) /  "matches.csv")
        statistics_dict = self.compute_blocking_statistics(candidate_set_df, golden_df, left_df, right_df)
        return statistics_dict
        # return candidate_set_df

    def compute_blocking_statistics(self, candidate_set_df, golden_df, left_df, right_df):
        #Now we have two data frames with two columns ltable_id and rtable_id
        # If we do an equi-join of these two data frames, we will get the matches that were in the top-K
        # merged_df = pd.merge(candidate_set_df, golden_df, on=['ltable_id', 'rtable_id'])

        left_num_tuples = len(left_df)
        right_num_tuples = len(right_df)
        statistics_dict = {
            "left_num_tuples": left_num_tuples,
            "right_num_tuples": right_num_tuples,
            "candidate_set": len(candidate_set_df),
            "recall": len(candidate_set_df) / len(golden_df),
            # "recall": len(merged_df) / len(golden_df),
            "cssr": len(candidate_set_df) / (left_num_tuples * right_num_tuples)
            }

        return statistics_dict
if __name__ == '__main__':

    n_hashes = 200
    band_size = 5
    shingle_size = 5
    n_docs = 3000
    max_doc_length = 400
    n_similar_docs = 10
    random.seed(42)

    clus = Cluster_Deepblocker()
    docs = clus.createflattenRawprofile(n_docs)
    print(docs[0])
    # print(docs[0].columns)
    cols_to_block = docs[0].columns.values.tolist()
    print(cols_to_block)
    print("using AutoEncoder embedding")
    tuple_embedding_model = AutoEncoderTupleEmbedding()
    topK_vector_pairing_model = ExactTopKVectorPairing(K=50)
    similar_docs = clus.do_blocking(docs[0], docs[1], cols_to_block=cols_to_block,
        tuple_embedding_model=tuple_embedding_model, vector_pairing_model=topK_vector_pairing_model)
    print(similar_docs)
    """df_dict = {}
    for pair1, pair2 in similar_docs:
        for k, v in pair1.items():
            key_var = f"{k}"+"_pair1"
            if not df_dict.get(key_var, None):
                df_dict[key_var] = []
                df_dict[key_var].append(v)
            else:
                df_dict[key_var].append(v)

        for k, v in pair2.items():
            key_var = f"{k}"+"_pair2"
            if not df_dict.get(key_var, None):
                df_dict[key_var] = []
                df_dict[key_var].append(v)
            else:
                df_dict[key_var].append(v)
    df = pd.DataFrame(df_dict)
    # df.drop_duplicates(inplace=True)
    df.to_csv(os.getcwd()+"/similiar.csv")

    r = float(n_hashes/band_size)
    similarity = (1/r)**(1/float(band_size))

    print("similarity: %f" % similarity)
    print("# Similar Pairs: %d" % len(similar_docs))

    if len(similar_docs) == n_similar_docs:
        print("Test Passed: All similar pairs found.")
    else:
        print("Test Failed.")"""