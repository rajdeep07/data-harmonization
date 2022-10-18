from typing import Any, Optional
import pandas as pd
from os import listdir
import os
import random
from pyspark.sql import DataFrame

from deep_blocker import DeepBlocker
from tuple_embedding_models import (
    AutoEncoderTupleEmbedding,
)
from vector_pairing_models import ExactTopKVectorPairing

from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
from data_harmonization.main.code.tiger.model.ingester.Rawentity import Rawentity
from data_harmonization.main.code.tiger.spark import SparkClass
from data_harmonization.main.resources import config as config_


class Deepblocker:
    """create cluster pairs using DeepBlocker"""

    # TODO: create wrapper for reading datasets
    # candidate_set_df = pd.DataFrame()
    # current_dir = os.path.dirname(os.path.realpath(__file__))
    # target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-3])
    # filenames = listdir(target_dir + "/data/")

    # # filenames = listdir(os.getcwd() + "/data_harmonization/main/data/")
    # csv_filenames = [filename for filename in filenames if filename.endswith(".csv")]
    # # rawProfiles = pd.DataFrame() # correct data structure here ?
    # rawProfiles = []
    # rawProfilesWithTokens = []
    # i = 0
    # for csv_file in csv_filenames:
    #     rawProfiles.append(
    #         pd.read_csv(os.getcwd() + f"/data_harmonization/main/data/{csv_file}")
    #     )
    #     # Cleansing of the data set
    #     rawProfilesWithTokens.append(
    #         rawProfiles[i].apply(
    #             lambda r: Sanitizer().toRawEntity(r, Rawentity), axis=1
    #         )
    #     )  # .filter(lambda p: p.id.isNotEmpty)
    #     i = i + 1

    # Flatten rawProfiles to fields which are only string / int
    # def createflattenRawprofile(self, n_docs: Optional[int]):
    #     flattenRawprofile = []
    #     id = 0
    #     flattenRawprofileDF = []
    #     for rawProfilesWithToken in self.rawProfilesWithTokens:
    #         for raw_ent in rawProfilesWithToken.sample(n_docs):
    #             semiflattenRawprofile = {}
    #             raw_dict = raw_ent.__dict__
    #             for k, v in raw_dict.items():
    #                 if not isinstance(v, (int, str, float)) and v:
    #                     for k1, v1 in v.__dict__.items():
    #                         semiflattenRawprofile[k1] = v1
    #                 else:
    #                     semiflattenRawprofile[k] = v
    #             flattenRawprofile.append(semiflattenRawprofile)
    #         flattenRawprofileDF.append(pd.DataFrame.from_dict(flattenRawprofile))
    #     return flattenRawprofileDF

    # def isNotEmpty(self, input: Optional[Any] = None) -> bool:
    #     if input is None:
    #         return False
    #     elif isinstance(input, str):
    #         return bool(input.strip())
    #     else:
    #         return False

    def do_blocking(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        cols_to_block: list,
        tuple_embedding_model,
        vector_pairing_model,
    ):
        left_df.show()
        right_df.show()
        # if "id" in cols_to_block:
        #     cols_to_block.remove("id")
        print(cols_to_block)
        pandas_left_df = left_df.toPandas()
        pandas_right_df = right_df.toPandas()
        pandas_left_df.set_index("id", drop=False, inplace=True)
        pandas_right_df.set_index("id", drop=False, inplace=True)
        print(pandas_left_df.head())
        print(pandas_right_df.head())
        db = DeepBlocker(tuple_embedding_model, vector_pairing_model)
        candidate_set_df = db.block_datasets(
            pandas_left_df, pandas_right_df, cols_to_block
        )
        print(candidate_set_df.head())
        # print("Writing model output file")
        # candidate_set_df.to_csv(
        #     self.target_dir + "/deepblocker_matching.csv", mode="w+"
        # )

        blocked_datasets = self.spark.get_sparkSession().createDataFrame(
            candidate_set_df
        )
        # save in database
        self.spark.write_to_database_from_df(
            "deepblocker_matching", blocked_datasets, "overwrite"
        )

        result_ = blocked_datasets.join(
            other=left_df, on=blocked_datasets.ltable_id == left_df.id
        ).drop(left_df.id)
        result = result_.join(other=right_df, on=result_.rtable_id == right_df.id).drop(
            right_df.id
        )
        result.show()

        # ltable_ids = candidate_set_df["ltable_id"]
        # ltable_ids.drop_duplicates(inplace=True)

        # rtable_ids = candidate_set_df["rtable_id"]
        # rtable_ids.drop_duplicates(inplace=True)

        # ltable = pandas_left_df.loc[ltable_ids]
        # ltable["ltable_id"] = candidate_set_df["ltable_id"]
        # rtable = pandas_right_df.loc[rtable_ids]
        # rtable["rtable_id"] = rtable.index

        # print("Generating result")
        # result = pd.merge(
        #     left=candidate_set_df, right=ltable, on="ltable_id", how="inner"
        # )
        # result = pd.merge(
        #     left=result,
        #     right=rtable,
        #     on="rtable_id",
        #     how="inner",
        #     suffixes=("_flna", "_pbna"),
        # )
        # result.to_csv(self.target_dir + "/deepblocker.csv", mode="w+")
        # result_df = self.spark.get_sparkSession().createDataFrame(result)
        self.spark.write_to_database_from_df("deepblocker", result, "overwrite")
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

    def prepare_data(self, table):
        self.spark = SparkClass()
        df = self.spark.read_from_database_to_dataframe(table)

        firstDF = df.sample(withReplacement=False, fraction=0.5)
        secondDF = df.subtract(firstDF)
        return firstDF, secondDF


if __name__ == "__main__":

    n_hashes = 200
    band_size = 5
    shingle_size = 5
    n_docs = 3000
    max_doc_length = 400
    n_similar_docs = 10
    random.seed(42)

    clus = Deepblocker()

    # docs = clus.createflattenRawprofile(n_docs)
    # cols_to_block = docs[0].columns.values.tolist()
    # print(cols_to_block)
    left_df, right_df = clus.prepare_data(config_.raw_entity_table)
    cols_to_block = left_df.columns
    print("using AutoEncoder embedding")
    tuple_embedding_model = AutoEncoderTupleEmbedding()
    topK_vector_pairing_model = ExactTopKVectorPairing(K=5)

    # pass 2 dataframes here
    similar_docs = clus.do_blocking(
        left_df,
        right_df,
        cols_to_block=cols_to_block,
        tuple_embedding_model=tuple_embedding_model,
        vector_pairing_model=topK_vector_pairing_model,
    )
    similar_docs.show()
    # print(similar_docs)
