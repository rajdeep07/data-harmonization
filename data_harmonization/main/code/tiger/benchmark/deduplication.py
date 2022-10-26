import argparse
import os
from re import finditer

import numpy as np
import pandas as pd
import pandas_dedupe

from data_harmonization.main.code.tiger.spark import SparkClass


class Deduplication:
    def __init__(self):
        self.raw_entity_table_name = "rawentity"

    def get_data(
        self, table: str = "rawentity", max_length: int = 2000
    ) -> pd.DataFrame:
        spark = SparkClass()
        df = spark.read_from_database_to_dataframe(table)
        pandas_df = df.toPandas()
        # if there are more than max_length records, take randomly sampled 20000 records
        if pandas_df.shape[0] > max_length:
            pandas_df = pandas_df.sample(n=max_length)
        return pandas_df

    def _clean_data(self, df: pd.DataFrame, column_names: list = []):
        if column_names and len(column_names) > 0:
            df = df[column_names]
        else:
            column_names = list(df.columns)
        if "cluster_id" in df.columns:
            df.drop(columns=["cluster_id"], inplace=True)
            # df.rename(columns={"cluster_id": "provided_cluster_id"})

        if "id" in column_names:
            column_names.remove("id")

        df = df.replace(r"^\s*$", np.nan, regex=True)
        # for col in columns:
        #     if df[col].isna().sum() > 0:
        #         columns.remove(col)
        #         df.drop(columns=[col], inplace=True)

        return df, column_names

    # This method is used for model training.
    def _run_model(self, df: pd.DataFrame, col_names: list = []):
        df_for_dedupe_model, col_names = self._clean_data(df, col_names)  # df.copy()
        print(col_names)
        final_model = pandas_dedupe.dedupe_dataframe(
            df_for_dedupe_model,
            col_names,
            threshold=0.7,
            canonicalize=True,
        )

        final_model = final_model[final_model["id"] != final_model["canonical_id"]]

        # Cleansing
        final_model = final_model.rename(columns={"cluster id": "cluster_id"})
        # final_model.sort_values(
        #     by=["cluster_id", "confidence"], ascending=True, inplace=True
        # )
        # Persist this in MYSQL + benckmark
        self._save_data_in_db(
            final_model[["id", "canonical_id", "cluster_id", "confidence"]], "benchmark"
        )
        print(self._get_statistics(df_for_dedupe_model, final_model))

        return

    def _save_data_in_db(self, df: pd.DataFrame, table: str):
        spark = SparkClass()
        spark_df = spark.get_sparkSession().createDataFrame(df)
        spark.write_to_database_from_df(table, spark_df, mode="overwrite")

    def _get_statistics(self, input_data: pd.DataFrame, model_output: pd.DataFrame):
        total_records = len(input_data.index)
        duplicates = len(model_output)
        number_of_clusters = model_output["cluster_id"].nunique()
        duplicates = (
            model_output.loc[model_output["confidence"] > 0.7]
            .groupby(by="cluster_id")["confidence"]
            .count()
            .sum()
        )
        cluster_with_max_duplicates = (
            model_output.loc[model_output["confidence"] > 0.7]
            .groupby(by="cluster_id")["confidence"]
            .count()
            .idxmax()
        )
        result = {
            "Total records": total_records,
            "Number of duplicate sets": number_of_clusters,
            "Duplicates": duplicates,
            "% duplicates": duplicates / total_records,
            "Cluster with maximum duplicates": cluster_with_max_duplicates,
        }
        return result

    def train(self, col_names: list = [], df=None):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-2])
        if os.path.isfile(
            target_dir + "/tiger/benchmark/dedupe_dataframe_learned_settings"
        ):
            os.remove(target_dir + "/tiger/benchmark/dedupe_dataframe_learned_settings")
        if os.path.isfile(
            target_dir + "/tiger/benchmark/dedupe_dataframe_training.json"
        ):
            os.remove(target_dir + "/tiger/benchmark/dedupe_dataframe_training.json")
        print("removed")
        if not df:
            df = self.get_data(self.raw_entity_table_name)
        return self._run_model(df, col_names)

    def predict(self, col_names: list = [], df=None):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-2])
        if not df:
            df = self.get_data(self.raw_entity_table_name)
        if not os.path.isfile(
            target_dir + "/tiger/benchmark/dedupe_dataframe_learned_settings"
        ):
            print("Cannot find dedupe_dataframe_learned_settings file")
        if not os.path.isfile(
            target_dir + "/tiger/benchmark/dedupe_dataframe_training.json"
        ):
            print("Cannot find dedupe_dataframe_training.json file")
        return self._run_model(df, col_names)


if __name__ == "__main__":
    dedupe = Deduplication()
    parser = argparse.ArgumentParser(
        description="Depuplication algorithm for creating benchmark table"
    )
    parser.add_argument(
        "-t", "--train", help="train the model", default=True, action="store_true"
    )
    parser.add_argument(
        "-p",
        "--predict",
        help="Predict from the model",
        default=False,
        action="store_true",
    )
    arg = parser.parse_args()
    # print(arg)
    # For training
    if arg.predict:
        print(f"Starting to predict....")
        dedupe.predict()
        print("We are done with prediction.")

    # For Prediction
    elif arg.train:
        print("Begin Active Learning.\nTraining the model")
        # df = dedupe.get_data("rawentity")
        dedupe.train()
        print("We are done with training.")
