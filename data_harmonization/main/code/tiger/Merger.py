from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list, col, udf
from pyspark.sql.types import StringType

import data_harmonization.main.resources.config as config_
from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass
from pyspark.sql.column import Column


class Merger:
    def __init__(self) -> None:
        self.spark = SparkClass()

    def get_data_from_database(
        self, table: str = config_.classification_table
    ) -> DataFrame:
        df = self.spark.read_from_database_to_dataframe(table)
        return df

    def do_merging(self, conneted_profiles=None):
        # fetch data from conneted_profiles if dataframe not provided
        if not conneted_profiles:
            conneted_profiles = self.spark.read_from_database_to_dataframe(
                config_.classification_table
            )

        # fetch raw entity table data
        raw_entities = self.spark.read_from_database_to_dataframe(
            config_.raw_entity_table
        )

        # join classifier and raw profiles table
        conneted_raw_entities = conneted_profiles.join(
            other=raw_entities, on="id", how="inner"
        )
        attributes = raw_entities.columns
        # attributes.remove("id")
        exprs = [collect_list(x).alias(x) for x in attributes]
        collected_profiles = conneted_raw_entities.groupBy("cluster_id").agg(*exprs)
        collected_profiles.show()

        def return_max_val(x):
            max_ = ""
            for elem in x:
                if len(elem) > len(max_):
                    max_ = elem

            return max_

        # Converting function to UDF
        return_max_length_val = udf(lambda z: return_max_val(z), StringType())
        # collected_profiles.withColumn("Name", returnmaxval(collected_profiles["Name"]))
        collected_profiles.withColumn(
            "cluster_Name", return_max_length_val(col("Name"))
        )
