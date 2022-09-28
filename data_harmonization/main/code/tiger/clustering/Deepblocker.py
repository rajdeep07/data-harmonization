from pathlib import Path
import re
from typing import Any, Optional
from data_harmonization.main.code.tiger.transformer.utils import StringSupport
from data_harmonization.main.code.tiger.transformer import (
    CityTransformer,
    NameTransformer,
    PostalAddressTransformer,
    StateTransformer,
)
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
from tuple_embedding_models import (
    AutoEncoderTupleEmbedding,
    CTTTupleEmbedding,
    HybridTupleEmbedding,
)
from vector_pairing_models import ExactTopKVectorPairing
import blocking_utils


class Deepblocker:
    """create cluster pairs using DeepBlocker"""

    # TODO: create wrapper for reading datasets
    candidate_set_df = pd.DataFrame()
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
        rawProfiles.append(
            pd.read_csv(os.getcwd() + f"/data_harmonization/main/data/{csv_file}")
        )
        # Cleansing of the data set
        rawProfilesWithTokens.append(
            rawProfiles[i].apply(lambda r: Sanitizer().toRawEntity(r), axis=1)
        )  # .filter(lambda p: p.id.isNotEmpty)
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

    def isNotEmpty(self, input: Optional[Any] = None) -> bool:
        if input is None:
            return False
        elif isinstance(input, str):
            return bool(input.strip())

    def do_blocking(
        self,
        left_df,
        right_df,
        cols_to_block,
        tuple_embedding_model,
        vector_pairing_model,
    ):
        db = DeepBlocker(tuple_embedding_model, vector_pairing_model)
        self.candidate_set_df = db.block_datasets(left_df, right_df, cols_to_block)

        print("Writing model output file")
        self.candidate_set_df.to_csv(
            self.target_dir + "/deepblocker_matching.csv", mode="w+"
        )

        ltable_ids = self.candidate_set_df["ltable_id"]
        ltable_ids.drop_duplicates(inplace=True)

        rtable_ids = self.candidate_set_df["rtable_id"]
        rtable_ids.drop_duplicates(inplace=True)

        ltable = left_df.loc[ltable_ids]
        ltable["ltable_id"] = self.candidate_set_df["ltable_id"]
        rtable = right_df.loc[rtable_ids]
        rtable["rtable_id"] = rtable.index

        print("Generating result")
        result = pd.merge(
            left=self.candidate_set_df, right=ltable, on="ltable_id", how="inner"
        )
        result = pd.merge(
            left=result,
            right=rtable,
            on="rtable_id",
            how="inner",
            suffixes=("_flna", "_pbna"),
        )
        result.to_csv(self.target_dir + "/deepblocker.csv", mode="w+")
        return result

    def compute_blocking_statistics(
        self, candidate_set_df, golden_df, left_df, right_df
    ):
        # Now we have two data frames with two columns ltable_id and rtable_id
        # If we do an equi-join of these two data frames, we will get the matches that were in the top-K
        # merged_df = pd.merge(candidate_set_df, golden_df, on=['ltable_id', 'rtable_id'])

        print("Comparinig model output to ground truth")
        csv_file = "benchmark.csv"
        golden_df = pd.read_csv(
            self.target_dir + f"/{csv_file}"
        )  # pd.read_csv(Path(folder_root) /  "matches.csv")
        # statistics_dict = self.compute_blocking_statistics(self.candidate_set_df, golden_df, left_df, right_df)
        # return statistics_dict

        left_num_tuples = len(left_df)
        right_num_tuples = len(right_df)
        statistics_dict = {
            "left_num_tuples": left_num_tuples,
            "right_num_tuples": right_num_tuples,
            "candidate_set": len(candidate_set_df),
            "recall": len(candidate_set_df) / len(golden_df),
            # "recall": len(merged_df) / len(golden_df),
            "cssr": len(candidate_set_df) / (left_num_tuples * right_num_tuples),
        }

        return statistics_dict


if __name__ == "__main__":

    n_hashes = 200
    band_size = 5
    shingle_size = 5
    n_docs = 3000
    max_doc_length = 400
    n_similar_docs = 10
    random.seed(42)

    clus = Deepblocker()
    docs = clus.createflattenRawprofile(n_docs)
    cols_to_block = docs[0].columns.values.tolist()
    print(cols_to_block)
    print("using AutoEncoder embedding")
    tuple_embedding_model = AutoEncoderTupleEmbedding()
    topK_vector_pairing_model = ExactTopKVectorPairing(K=5)
    similar_docs = clus.do_blocking(
        docs[0],
        docs[1],
        cols_to_block=cols_to_block,
        tuple_embedding_model=tuple_embedding_model,
        vector_pairing_model=topK_vector_pairing_model,
    )
    print(similar_docs)
