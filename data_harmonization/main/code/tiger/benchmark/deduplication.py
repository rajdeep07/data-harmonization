import os
import sys
from os import listdir

import numpy as np
import pandas as pd
import pandas_dedupe
import xlrd


class Deduplication:

    # This method is used for model training.
    def model_training(self):

        # Generate dataframe
        current_dir = os.path.dirname(os.path.realpath(__file__))
        target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-3])
        filenames = listdir(target_dir + "/data/")
        csv_filenames = [
            filename
            for filename in filenames
            if filename.endswith(".csv") and not filename.startswith("benchmark")
        ]

        # Read from MYSQL + RawEntity ==> PySpark DF
        # PySparkDf to toPandas()
        # len(PySparkDf) < 20,000 Records :: Take the entire dataset
        # If not, randomly sample 20k records.
        # if not None, features_for_deduplication [User Provided] Subset pandas dataframe
        # if None, use all columns

        dataframe = pd.DataFrame()  # correct data structure here ?
        for csv_file in csv_filenames:
            dataframe = dataframe.append(pd.read_csv(target_dir + f"/data/{csv_file}"))

        # Run Dedupe Model
        # DataFrame :: features_for_deduplication + id columns
        dataframe = dataframe[["Name", "Address", "City", "State", "Zip", "id"]]  # "source"
        final_model = pandas_dedupe.dedupe_dataframe(
            # features_for_deduplication
            dataframe, ["Name", "Address", "City", "State", "Zip"]
        )

        # Cleansing
        final_model = final_model.rename(columns={"cluster id": "cluster_id"})
        final_model.sort_values(
            by=["cluster_id", "confidence"], ascending=True, inplace=True
        )

        # Output the data as csv
        print("We are writing dataset on")
        # Persist this in MYSQL + benckmark
        final_model.to_csv(target_dir + "/data/benchmark.csv", mode="w+")


        # Few Statistics
        # Total_Records
        # Duplicates
        # %_duplicate = Duplicates/ Total_Records

        # Show 1 cluster which has maximum duplicates


        return


if __name__ == "__main__":


    # For Training
    # rm dedupe_dataframe_training.json
    # rm dedupe_dataframe_learned_settings

    dedupe = Deduplication()
    print("Begin Active Learning.")
    dedupe.model_training()
    print("We are done with training.")

    # For Prediction
