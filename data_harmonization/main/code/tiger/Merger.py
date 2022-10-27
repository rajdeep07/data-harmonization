from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_list, concat_ws, udf
from pyspark.sql.types import StringType

import data_harmonization.main.resources.config as config_
from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass
from data_harmonization.main.resources.log4j import Logger


class Merger:
    """Merge all connected profile ids from Graph
    with all other attributes from raw entities"""

    def __init__(self) -> None:
        """Setting up initial variables"""
        self.spark = SparkClass()
        self.logger = Logger(name="merger")

    def _get_data(self, conneted_profiles=None) -> tuple:
        """Fetch connected profile and raw entities.
        If connected profile is not provided fetch it from database.

        :param conneted_profiles: spark dataframe of all conneted profiles
        from graph object i.e from Mapping module output
        :return: dataframes of connected profiles and raw entities
        """
        # if conneted_profiles not provided fetch it from database
        if not conneted_profiles:
            self.logger.log(
                level="INFO",
                msg="Fetching connected profiles from database"
            )
            conneted_profiles = self.spark.read_from_database_to_dataframe(
                config_.graph_connected_components_table
            )

        # fetch raw entity table data
        self.logger.log(
            level="INFO",
            msg="Fetching raw entities from database"
        )
        raw_entities = self.spark.read_from_database_to_dataframe(
            config_.raw_entity_table
        )

        if "cluster_id" in raw_entities.columns:
            raw_entities = raw_entities.drop("cluster_id")
        return conneted_profiles, raw_entities

    def do_merging(self, conneted_profiles=None) -> DataFrame:
        """fetch data from conneted_profiles if dataframe not provided

        :param conneted_profiles: spark dataframe of all conneted profiles
        from graph object i.e from Mapping module output
        :return: merged dataframe of all the duplicate ids
        with all other attribute values
        """
        # Fetch connected profiles and raw entities
        conneted_profiles, raw_entities = self._get_data(conneted_profiles)

        # join classifier and raw profiles table
        self.logger.log(
            level="INFO",
            msg="Merging connected profile with raw entities"
        )
        conneted_raw_entities = conneted_profiles.join(
            other=raw_entities,
            on=conneted_profiles.id == raw_entities.id, how="inner"
        ).drop(raw_entities.id)
        attributes = raw_entities.columns
        exprs = [collect_list(x).alias(x) for x in attributes]
        self.logger.log(
            level="INFO",
            msg="Collecting all profiles belong to same cluster id"
        )
        collected_profiles = conneted_raw_entities.groupBy("cluster_id") \
            .agg(*exprs)

        def _return_max_val(x):
            """Return x if x is int or float or str.
            If x is iterable of string type elements return the string
            whichever has maximum length.
            If x is iterable of int or float return maximum value

            :param x: int or float or iterable of str or int or float
            :return: string with maximium length or maximum value for
            int or float
            """
            max_ = ""
            # x is None or x is int or float or str type
            if not x or type(x) in (int, float, str):
                return x
            # if x is iterable and len is 0
            if len(x) == 0:
                return x
            # if x is iterable and elements are str type
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

        # collect data from list based on maximum length if it is str type
        # otherwise take maximum value
        collected_profiles_id_cluster = collected_profiles.select(
            ["id", "cluster_id"]
        )
        collected_profiles_ = collected_profiles.select(
            *[
                return_max_length_val(col(col_name)).name(col_name)
                for col_name in collected_profiles.columns
                if col_name != "id"
            ]
        )

        # join all ids(including duplicate entities ids) with other attributes
        result = collected_profiles_id_cluster.join(
            other=collected_profiles_,
            on=collected_profiles_id_cluster.cluster_id
            == collected_profiles_.cluster_id,
            how="inner",
        ).drop(collected_profiles_id_cluster.cluster_id)

        # convert id column from list type to string type
        result_ = result.withColumn("id", concat_ws(",", result.id))
        # writing merged data into database
        self.logger.log(
            level="INFO",
            msg=f"Writing all merged data in {config_.merged_table} "
            + "table in database"
        )
        self.spark.write_to_database_from_df(
            config_.merged_table, result_, "overwrite"
        )
        return result


if __name__ == "__main__":
    merger = Merger()
    merger.do_merging()
