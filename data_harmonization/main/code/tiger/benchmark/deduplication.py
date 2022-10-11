import os
import sys
from os import listdir

import numpy as np
import pandas as pd
import pandas_dedupe
import xlrd

from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass


class Deduplication:
    def get_data(self, table: str = "Raw_Entity") -> pd.DataFrame:
        spark = SparkClass()
        df = spark.read_from_database_to_dataframe(table)
        pandas_df = df.toPandas()
        # if there are more than 20000 records, take randomly sampled 20000 records
        if len(pandas_df.index) > 20000:
            pandas_df = pandas_df.sample(n=20000)
        return pandas_df

    # This method is used for model training.
    def model_training(self, df: pd.DataFrame, col_names: list = []):

        # Generate dataframe
        # current_dir = os.path.dirname(os.path.realpath(__file__))
        # target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-3])
        # filenames = listdir(target_dir + "/data/")
        # csv_filenames = [
        #     filename
        #     for filename in filenames
        #     if filename.endswith(".csv") and not filename.startswith("benchmark")
        # ]

        # Read from MYSQL + RawEntity ==> PySpark DF
        # PySparkDf to toPandas()
        # len(PySparkDf) < 20,000 Records :: Take the entire dataset
        # If not, randomly sample 20k records.
        # if not None, features_for_deduplication [User Provided] Subset pandas dataframe
        # if None, use all columns

        # dataframe = pd.DataFrame()  # correct data structure here ?
        # for csv_file in csv_filenames:
        #     dataframe = dataframe.append(pd.read_csv(target_dir + f"/data/{csv_file}"))

        # Run Dedupe Model
        # DataFrame :: features_for_deduplication + id columns
        # dataframe = dataframe[
        #     ["Name", "Address", "City", "State", "Zip", "id"]
        # ]  # "source"

        # remove id column from dataframe
        # df_for_dedupe_model = df.drop("id")
        df_for_dedupe_model = df.copy()
        if col_names and len(col_names) > 0:
            # df_for_dedupe_model = df_for_dedupe_model.columns.intersection(col_names)
            df_for_dedupe_model = df_for_dedupe_model[col_names]
        else:
            col_names = list(df_for_dedupe_model.columns)
        if "id" in col_names:
                col_names.remove("id")
        print(col_names)
        final_model = pandas_dedupe.dedupe_dataframe(
            # features_for_deduplication
            # dataframe, ["Name", "Address", "City", "State", "Zip"]
            df_for_dedupe_model,
            col_names,
        )

        # Cleansing
        final_model = final_model.rename(columns={"cluster id": "cluster_id"})
        final_model.sort_values(
            by=["cluster_id", "confidence"], ascending=True, inplace=True
        )

        # Output the data as csv
        print("We are writing dataset on")
        # Persist this in MYSQL + benckmark
        self._save_data_in_db(final_model, "benchmark")
        # final_model.to_csv(target_dir + "/data/benchmark.csv", mode="w+")

        # Few Statistics
        # Total_Records
        # Duplicates
        # %_duplicate = Duplicates/ Total_Records
        print(self._get_statistics(df_for_dedupe_model, final_model))
        # Show 1 cluster which has maximum duplicates

        return

    def _save_data_in_db(self, df: pd.DataFrame, table: str):
        spark = SparkClass()
        spark_df = spark.get_sparkSession().createDataFrame(df)
        spark.write_to_database_from_df(table, spark_df, mode="overwrite")

    def _get_statistics(self, input_data: pd.DataFrame, model_output: pd.DataFrame):
        total_records = len(input_data.index)
        duplicates = len(model_output)
        # print("Total records : " + str(total_records))
        # print("Duplicaes : " + str(duplicates))
        # print("% duplicates : " + str(duplicates / total_records))

        result = {
            "Total records": total_records,
            "Duplicates": duplicates,
            "% duplicates": duplicates / total_records,
        }
        # Show 1 cluster which has maximum duplicates

        return result


if __name__ == "__main__":

    # For Training
    # rm dedupe_dataframe_training.json
    # rm dedupe_dataframe_learned_settings

    dedupe = Deduplication()
    print("Begin Active Learning.")
    df = dedupe.get_data("Raw_Entity")
    dedupe.model_training(df)
    print("We are done with training.")

    # For Prediction
