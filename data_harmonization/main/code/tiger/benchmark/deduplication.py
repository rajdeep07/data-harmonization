import argparse
import os

import numpy as np
import pandas as pd
import pandas_dedupe

import data_harmonization.main.resources.config as config_
from data_harmonization.main.code.tiger.spark import SparkClass
from data_harmonization.main.resources.log4j import Logger


class Deduplication:
    """Run pandas deduplication model"""

    def __init__(self) -> None:
        """Setting up initial variables"""
        self.raw_entity_table_name = "rawentity"
        self.spark = SparkClass()
        self.logger = Logger(name="deduplication")

    def get_data(
        self, table: str = config_.raw_entity_table, max_length: int = 20000
    ) -> pd.DataFrame:
        """Fetch data from database table.
        If there are more than max_length records,
        take randomly sampled max_length records

        :param table: table name in the database
        :param max_length: maximum number of records
        :return: fethed values from database
        """
        self.logger.log(level="INFO", msg="Fetching data from ")
        df = self.spark.read_from_database_to_dataframe(table)
        pandas_df = df.toPandas()
        # if there are more than max_length records,
        # take randomly sampled 20000 records
        if pandas_df.shape[0] > max_length:
            pandas_df = pandas_df.sample(n=max_length)
        return pandas_df

    def _clean_data(self, df: pd.DataFrame, column_names: list) -> tuple:
        """Clean data from the dataframe to feed it in the deduplication model.
        Also process the column names to consider those columns
        only for deduplication

        :param df: all data in the shape of pandas dataframe
        :param column_names: list of columns those should be considered
        for the deduplication
        """
        self.logger.log(level="INFO", msg="Cleaning data")
        if column_names and len(column_names) > 0:
            df = df[column_names]
        else:
            column_names = list(df.columns)
        if "cluster_id" in df.columns:
            df.drop(columns=["cluster_id"], inplace=True)

        if "id" in column_names:
            column_names.remove("id")

        df = df.replace(r"^\s*$", np.nan, regex=True)

        return df, column_names

    # This method is used for model training.
    def _run_model(self, df: pd.DataFrame, col_names: list) -> None:
        """Run deduplication model

        :param df: data on which deduplication will be run
        :param col_names: list of columns those should be considered
        for the deduplication
        """
        df_for_dedupe_model, col_names = self._clean_data(df, col_names)
        print(col_names)
        self.logger.log(level="INFO", msg="Running the deduplication model")
        final_model = pandas_dedupe.dedupe_dataframe(
            df_for_dedupe_model,
            col_names,
            threshold=0.7,
            canonicalize=True,
        )

        final_model = final_model[final_model["id"] != final_model[
            "canonical_id"]
        ]

        # Cleansing
        final_model = final_model.rename(columns={"cluster id": "cluster_id"})

        self._save_data_in_db(
            final_model[["id", "canonical_id", "cluster_id", "confidence"]],
            config_.benchmark_table,
        )
        print(self._get_statistics(df_for_dedupe_model, final_model))

    def _save_data_in_db(self, df: pd.DataFrame, table: str) -> None:
        """Save data in the database

        :param df: data to be saved
        :param table: table name where data will be saved
        """
        self.logger.log(
            level="INFO",
            msg="Writing connected compenents in "
            + f"{table} table in database"
        )
        spark_df = self.spark.get_sparkSession().createDataFrame(df)
        self.spark.write_to_database_from_df(table, spark_df, mode="overwrite")

    def _get_statistics(
        self, input_data: pd.DataFrame, model_output: pd.DataFrame
    ) -> dict:
        """Calculate statistics from input data and deduplication model output

        :param input_data: data that was fed to deduplication model
        :param model_output: deduplication model output data
        :return: calculated statistics
        """
        self.logger.log(level="INFO", msg="Calculating statistics")
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

    def train(self, col_names=None, df=None) -> None:
        """Train the deduplication model

        :param col_names: list of columns those should be considered
        for the deduplication
        :param df: data on which deduplication will be run
        """
        if not col_names:
            col_names = []
        current_dir = os.path.dirname(os.path.realpath(__file__))
        target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-2])
        if os.path.isfile(
            target_dir + "/tiger/benchmark/dedupe_dataframe_learned_settings"
        ):
            os.remove(
                target_dir
                + "/tiger/benchmark/dedupe_dataframe_learned_settings"
            )
        if os.path.isfile(
            target_dir + "/tiger/benchmark/dedupe_dataframe_training.json"
        ):
            os.remove(
                target_dir
                + "/tiger/benchmark/dedupe_dataframe_training.json"
            )
        self.logger.log(
            level="INFO",
            msg="Removed trained model files if they were present"
        )
        if not df:
            df = self.get_data(self.raw_entity_table_name)
        self._run_model(df, col_names)

    def predict(self, col_names=None, df=None) -> None:
        """Predict using the deduplication model

        :param col_names: list of columns those should be considered
        for the deduplication
        :param df: data on which deduplication will be run
        """
        if not col_names:
            col_names = []
        current_dir = os.path.dirname(os.path.realpath(__file__))
        target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-2])
        if not df:
            df = self.get_data(self.raw_entity_table_name)
        if not os.path.isfile(target_dir + "/tiger/benchmark/dedupe_dataframe_learned_settings"):
            print("Cannot find dedupe_dataframe_learned_settings file")
        if not os.path.isfile(target_dir + "/tiger/benchmark/dedupe_dataframe_training.json"):
            print("Cannot find dedupe_dataframe_training.json file")
        self._run_model(df, col_names)


if __name__ == "__main__":
    dedupe = Deduplication()
    parser = argparse.ArgumentParser(
        description="Depuplication algorithm for creating benchmark table"
    )
    parser.add_argument(
        "-t",
        "--train",
        help="train the model",
        default=True,
        action="store_true"
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
        print("Starting to predict....")
        dedupe.predict()
        print("We are done with prediction.")

    # For Prediction
    elif arg.train:
        print("Begin Active Learning.\nTraining the model")
        # df = dedupe.get_data("rawentity")
        dedupe.train()
        print("We are done with training.")
