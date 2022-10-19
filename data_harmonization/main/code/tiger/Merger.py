from unicodedata import name
from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list, col, udf, concat_ws
from pyspark.sql.types import StringType

# import pyspark.sql.fu import

import data_harmonization.main.resources.config as config_
from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass
from pyspark.sql.column import Column


class Merger:
    def __init__(self) -> None:
        self.spark = SparkClass()

    def do_merging(self, conneted_profiles=None) -> DataFrame:
        # fetch data from conneted_profiles if dataframe not provided
        if not conneted_profiles:
            conneted_profiles = self.spark.read_from_database_to_dataframe(
                config_.graph_connected_components_table
            )
        # conneted_profiles.show()
        # fetch raw entity table data
        raw_entities = self.spark.read_from_database_to_dataframe(
            config_.raw_entity_table
        )
        # raw_entities.show()
        if "cluster_id" in raw_entities.columns:
            raw_entities = raw_entities.drop("cluster_id")

        # join classifier and raw profiles table
        conneted_raw_entities = conneted_profiles.join(
            other=raw_entities, on=conneted_profiles.id == raw_entities.id, how="inner"
        ).drop(raw_entities.id)
        # conneted_raw_entities.show()
        attributes = raw_entities.columns
        # attributes.remove("id")
        exprs = [collect_list(x).alias(x) for x in attributes]
        # a.show()
        collected_profiles = conneted_raw_entities.groupBy(
            "cluster_id").agg(*exprs)
        # collected_profiles.show()
        # print("collected_profiles : ", str(collected_profiles.count()))

        def _return_max_val(x):
            if type(x) in (int, float):
                return x
            max_ = ""
            if not x:
                return x
            else:
                if len(x) == 0:
                    return x
                else:
                    if not isinstance(x[0], str):
                        max_ = 0
            for elem in x:
                if isinstance(elem, str):
                    if len(elem) > len(max_):
                        max_ = elem
                else:
                    if elem > max_:
                        max_ = elem

            return max_

        # Converting function to UDF
        return_max_length_val = udf(lambda z: _return_max_val(z), StringType())

        # collect data from list based on maximum length if it is str type otherwise take maximum value
        collected_profiles.show()
        collected_profiles_1 = collected_profiles.select(["id", "cluster_id"])
        # collected_profiles_1 = collected_profiles.groupBy("cluster_id").agg(D.coll)
        collected_profiles_ = collected_profiles.select(
            *[
                return_max_length_val(col(col_name)).name(col_name)
                for col_name in collected_profiles.columns
                if col_name != "id"
            ]
        )
        # df
        #     .groupby("id")
        #     .agg(F.collect_set("code"),
        #         F.collect_list("name"))

        result = collected_profiles_1.join(
            other=collected_profiles_,
            on=collected_profiles_1.cluster_id == collected_profiles_.cluster_id,
            how="inner",
        ).drop(collected_profiles_1.cluster_id)
        result.show()
        # collected_profiles.show()
        # writing merged data into database
        result_ = result.withColumn("id", concat_ws(",", result.id))
        result_.show()
        self.spark.write_to_database_from_df(
            config_.merged_table, result_, "overwrite")
        return result


if __name__ == "__main__":
    merger = Merger()
    merger.do_merging()
