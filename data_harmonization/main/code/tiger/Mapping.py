import os
import sys
from typing import Optional

from graphframes import *
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import data_harmonization.main.resources.config as config_
from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass

# from pyspark.sql.types import StringType, StructField, StructType

# TODO: While running also pass package parameter to get appropriate packges.
# ./bin/pyspark --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11


class Mapping:
    def __init__(self, database_name, table_name):
        self.database_name = database_name
        self.table_name = table_name
        self.App_Name = config_.APP_NAME
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        self.spark = SparkClass()

    def _getAllDuplicate(self, df):
        """Get all duplicates from dataframe based on isMatch column

        :return: set of sums of ids of duplicate data
        """
        # https://mungingdata.com/pyspark/column-to-list-collect-tolocaliterator/
        merges = df.select("leftId", "rightId", "isMatch").filter(
            df.isMatch == 1
        )
        list1 = merges.select("leftId").toPandas()["leftId"]
        list2 = merges.select("rightId").toPandas()["rightId"]
        # a = list1 + list2
        # b = set(list1 + list2)
        return set(list1 + list2)

    def _getAllProfiles(self, entities: DataFrame):
        """Get all ids from dataframe id column

        :return: set of all ids
        """
        # profile_ids = set()
        # entities -> list of pyspark dataframes
        # for entity in entities:
        #     profile_ids.add(entity.select("id").toPandas()["id"])
        profile_ids = set(
            entities.select("id").rdd.flatMap(lambda x: x).collect()
        )
        return profile_ids

    # def _getLocalVertices(self, emptyRDD, entities: list):
    def _getLocalVertices(self, emptyRDD, entities: DataFrame):
        # schema = StructType(
        #     [
        #         StructField("id", StringType(), True),
        #         StructField("Name", StringType(), True),
        #     ]
        # )
        print("raw count: ", entities.count())
        # vertexDF = emptyRDD.toDF(schema)
        node_properties = []
        # for entity in entities:
        #     entity = entity.withColumn("Name", F.concat(*node_properties))
        #     vertexDF = vertexDF.union(entity)
        vertexDF = entities.withColumn("name", F.concat(*node_properties))
        vertexDF.show()
        print("vertex count : ", vertexDF.count())
        return vertexDF

    def _getLocalEdges(self, df: DataFrame):
        """Get edges from dataframe

        :return: edges as  spark dataframe with columns named src, dst and action
        """
        edgesDF = (
            df.select("leftId", "rightId", "isMatch")
            .withColumnRenamed("leftId", "src")
            .withColumnRenamed("rightId", "dst")
            .withColumnRenamed("isMatch", "action")
        )
        return edgesDF

    def get_data_from_database(self, table: str) -> DataFrame:
        """Fetch values from database table

        :return: spark dataframe of table values
        """
        df = self.spark.read_from_database_to_dataframe(table)
        return df

    def graphObject(
        self, raw_profiles_df: Optional[DataFrame] = None
    ) -> GraphFrame:
        """Create GraphFrame object using vertices and edges.
        Vertices as raw profiles and edges from classification output

        :return: GraphFrame object
        """

        # if raw_profiles_df is not provided, fetch it from database
        if not raw_profiles_df:
            raw_profiles_df = self.get_data_from_database(
                config_.raw_entity_table
            )
        # entities = raw_profiles_df.select("id", "Name")

        # assign raw profiles as vertices
        v = raw_profiles_df

        # Get all Vertices with appropriate node properties
        # emptyRDD = spark.get_sparkSession().sparkContext.emptyRDD()
        # v = self._getLocalVertices(emptyRDD, entities)

        # Get all edges with appropriate edge for only matches
        mergesDF = self.spark.read_from_database_to_dataframe(self.table_name)
        e = self._getLocalEdges(mergesDF)

        # Create a graph
        g = GraphFrame(v, e)  # Graphframes(v, e)

        return g

    def saveConnectedComponents(self, g: GraphFrame) -> DataFrame:
        self.spark.get_sparkSession().sparkContext.setCheckpointDir(
            "data_harmonization/main/code/tiger/checkpoints"
        )
        df = g.connectedComponents()
        df = df.select(["id", "component"]).withColumnRenamed(
            "component", "cluster_id"
        )
        self.spark.write_to_database_from_df(
            config_.graph_connected_components_table, df, mode="overwrite"
        )
        return df

    def getScore(self, entities, df) -> float:
        """Print few statistics and return percentage of duplicate data

        :return: duplicate percentage
        """
        total_profiles = len(self._getAllProfiles(entities))
        duplicated_profiles = len(self._getAllDuplicate(df))
        duplicates_percentages = duplicated_profiles / total_profiles
        print(
            f"Total duplicated identified are, {duplicated_profiles} \
                among {total_profiles} total profiles, i.e. "
            f"{duplicates_percentages}% duplicates."
        )
        return duplicates_percentages

    # read potential matches from DB [Classifier Output]
    # [leftId <-> rightId <-> 1/0] 1 means match, 0 means not a match
    # Presume output from classifier as
    """
    *(1000378, 6521361, 0)
    *(1000378, 6521273, 1)
    *(1000378, 6521312, 1)
    *(1000378, 6521334, 0)
    *(1000378, 6521279, 0)
    *(1000378, 6521288, 1)
    *(1000378, 6521286, 1)
    *(1000378, 6521303, 0)
    """


if __name__ == "__main__":

    # ("280331830321684367756600197359728360720", "142228286082468145063900539212600403481", 1)
    # ("283843806261955534091170717768873091132", "311546418045301667329437731856517013363", 0)
    # ("256861333466236813781525143800530106090", "242056639072954036254798401269530940971", 0)
    # ("73683139853818832972964307434266257283", "154516104898211308948543536420190142835", 1)
    # ("311546418045301667329437731856517013363", "154516104898211308948543536420190142835", 1);
    # ("283843806261955534091170717768873091132", "154516104898211308948543536420190142835", 1);

    # table name from Mysql , Output from classifier
    table_name = config_.classification_table
    database_name = config_.DB_NAME
    map = Mapping(database_name, table_name)
    g = map.graphObject()
    g.cache()
    df = map.saveConnectedComponents(g)
    df.show()
    # g.vertices.show()
    # g.edges.show()
    # ## Check the number of edges of each vertex
    # g.degrees.show()

    # Calculate complete pairs : triangle counts
    # triangle_count = g.triangleCount()
    # triangle_count.show()

    # Calculate all sub_graphs : connected components / strongly connected components
    # strongly_connected_components = g.stronglyConnectedComponents(maxIter=10)
    # strongly_connected_components.show()
    # SparkClass().get_sparkSession().sparkContext.setCheckpointDir(
    #     "data_harmonization/main/code/tiger/checkpoints"
    # )
    # df1 = g.connectedComponents()
    # df1.show()
    # Drop singular un-matched profiles : dropIsolatedVertices
    # final = g.dropIsolatedVertices()
    # print(final)

    # print score
    raw_entities = map.get_data_from_database(config_.raw_entity_table)
    classification_table = map.get_data_from_database(table_name)
    map.getScore(raw_entities, classification_table)
