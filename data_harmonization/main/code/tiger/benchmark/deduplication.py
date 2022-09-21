import pandas_dedupe
import pandas as pd
import numpy as np
import xlrd
from os import listdir
import os
import sys


class Deduplication():

    # This method is used for model training.
    def model_training(self):

        # Generate dataframe
        current_dir = os.path.dirname(os.path.realpath(__file__))
        target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-3])
        filenames = listdir(target_dir + "/data/")
        csv_filenames = [filename for filename in filenames if filename.endswith(".csv") and not filename.startswith("benchmark")]
        dataframe = pd.DataFrame()  # correct data structure here ?
        for csv_file in csv_filenames:
            dataframe = dataframe.append(pd.read_csv(target_dir + f"/data/{csv_file}"))

        # Run Dedupe Model
        dataframe = dataframe[["Name", "Address", "City", "State","Zip","source"]]
        final_model = pandas_dedupe.dedupe_dataframe(dataframe,
                                                     ["Name", "Address", "City", "State","Zip"])

        # Cleansing
        final_model = final_model.rename(columns={"cluster id": "cluster_id"})

        # Output the data as csv
        print("We are writing dataset on")
        final_model.to_csv(target_dir + "/data/benchmark.csv", mode='w+')

        return
        

if __name__ == "__main__":

    dedupe = Deduplication()
    print("Begin Active Learning.")
    dedupe.model_training()
    print("We are done with training.")


