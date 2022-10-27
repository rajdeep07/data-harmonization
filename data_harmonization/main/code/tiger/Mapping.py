import os
import sys

from graphframes import GraphFrame
from pyspark.sql import DataFrame

import data_harmonization.main.resources.config as config_
from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass
from data_harmonization.main.resources.log4j import Logger

# TODO: While running also pass package parameter to get appropriate packges.
# ./bin/pyspark --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11


class Mapping:
    """Create Map object taking raw entities
    as vertices and classification output data as edges
    """

    def __init__(self, database_name, table_name) -> None:
        """Setting up initial variables

        Parameters
        ----------
        database_name
            Name of the database that will be used
        table_name
            Name of the table where output
            from classification model has been saved
        """
        self.database_name = database_name
        self.table_name = table_name
        self.App_Name = config_.APP_NAME
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        self.spark = SparkClass()
        self.logger = Logger(name="mapping")

    def _getAllDuplicate(self, df: DataFrame) -> set:
        """Get all duplicates from dataframe based on isMatch column

        Parameters
        ----------
        df
            spark dataframe with all data.
            It should have leftId, rightId, isMatch columns.

        Returns
        -------
        set
            set of sums of ids of duplicate data
        """
        self.logger.log(level="INFO", msg="Fetching all duplicates")
        merges = df.select("leftId", "rightId", "isMatch") \
            .filter(df.isMatch == 1)
        list1 = merges.select("leftId").toPandas()["leftId"]
        list2 = merges.select("rightId").toPandas()["rightId"]
        return set(list1 + list2)

    def _getAllProfiles(self, entities: DataFrame) -> set:
        """Get all ids from dataframe id column

        Parameters
        ----------
        entities
            spark dataframe that will be used here.
            It should have id column

        Returns
        -------
        set
            set of all ids
        """
        self.logger.log(level="INFO", msg="Fetching all ids from data")
        profile_ids = set(
            entities.select("id").rdd.flatMap(lambda x: x).collect()
        )
        return profile_ids

    def _getLocalEdges(self, df) -> DataFrame:
        """Get edges from dataframe

        Parameters
        ----------
        df
            spark dataframe that will be used here

        Returns
        -------
        DataFrame
            edges as spark dataframe with columns
            named src, dst and action
        """
        self.logger.log(level="INFO", msg="Creating edges")
        edgesDF = (
            df.select("leftId", "rightId", "isMatch")
            .withColumnRenamed("leftId", "src")
            .withColumnRenamed("rightId", "dst")
            .withColumnRenamed("isMatch", "action")
        )
        return edgesDF

    def graphObject(self, raw_profiles_df=None) -> GraphFrame:
        """Create GraphFrame object using vertices and edges.
        Vertices as raw profiles and edges from classification output

        Parameters
        ----------
        raw_profiles_df
            raw entities dataframe

        Returns
        -------
        GraphFrame
            GraphFrame object
        """
        # if raw_profiles_df is not provided, fetch it from database
        if not raw_profiles_df:
            raw_profiles_df = self.spark.read_from_database_to_dataframe(
                config_.raw_entity_table
            )

        # assign raw profiles as vertices
        v = raw_profiles_df

        # Get all edges with appropriate edge for only matches
        mergesDF = self.spark.read_from_database_to_dataframe(self.table_name)
        e = self._getLocalEdges(mergesDF)

        # Create a graph
        self.logger.log(level="INFO", msg="Creating graph object")
        g = GraphFrame(v, e)  # Graphframes(v, e)

        return g

    def saveConnectedComponents(self, g: GraphFrame) -> DataFrame:
        """Get connected components from graph object
        and save them in the database

        Parameters
        ----------
        g
            graph object from where connected components will be fethced

        Returns
        -------
        DataFrame
            all connected components
        """
        self.spark.get_sparkSession().sparkContext.setCheckpointDir(
            "data_harmonization/main/code/tiger/checkpoints"
        )
        df = g.connectedComponents()
        df = df.select(["id", "component"]).withColumnRenamed(
            "component", "cluster_id"
        )
        self.logger.log(
            level="INFO",
            msg="Writing connected compenents in "
            + f"{config_.graph_connected_components_table} table in database"
        )
        self.spark.write_to_database_from_df(
            config_.graph_connected_components_table, df, mode="overwrite"
        )
        return df

    def getScore(self, entities: DataFrame, df: DataFrame) -> float:
        """Print few statistics and return percentage of duplicate data

        Parameters
        ----------
        entities
            all raw entities
        df
            spark dataframe with leftId, rightId, isMatch columns

        Returns
        -------
        float
            duplicate percentage
        """
        self.logger.log(level="INFO", msg="Calculating statistics")
        total_profiles = len(self._getAllProfiles(entities))
        duplicated_profiles = len(self._getAllDuplicate(df))
        duplicates_percentages = duplicated_profiles / total_profiles
        print(
            f"Total duplicated identified are, {duplicated_profiles} "
            f"among {total_profiles} total profiles, i.e. "
            f"{duplicates_percentages}% duplicates."
        )
        return duplicates_percentages


if __name__ == "__main__":

    # table name from Mysql , Output from classifier
    table_name = config_.classification_table
    database_name = config_.DB_NAME
    map = Mapping(database_name, table_name)
    g = map.graphObject()
    g.cache()
    df = map.saveConnectedComponents(g)
    # print score
    raw_entities = SparkClass().read_from_database_to_dataframe(
        config_.raw_entity_table
    )
    classification_table = SparkClass() \
        .read_from_database_to_dataframe(table_name)
    map.getScore(raw_entities, classification_table)
