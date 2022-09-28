from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass
from pyspark import SparkContext
from pyspark.sql import Column, DataFrame, SQLContext
from pyspark.storagelevel import StorageLevel
from graphframes import *
from graphframes.lib import Pregel
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerField
import netifaces
import data_harmonization.main.resources.config as config

App_Name = "data_harmonization"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# TODO: While running also pass package parameter to get appropriate packges.
# ./bin/pyspark --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11

class Mapping(database_name, table_name):
    def __init__(self, database_name, table_name):
        self.database_name = database_name
        self.table_name = table_name

    def getAllDuplicate(self, df):
        # https://mungingdata.com/pyspark/column-to-list-collect-tolocaliterator/
        merges = df.select('leftId', 'rightId', 'isMatch').filter(col('isMatch').contains(1))
        list1 = merges.select('leftId').toPandas()['leftId']
        list2 = merges.select('rightId').toPandas()['rightId']
        return set(list1+list2)

    def getAllProfiles(self, entities: list):

        profile_ids = set()
        # entities -> list of pyspark dataframes
        for entity in entities:
            profile_ids.add(entity.select('id').toPandas()['id'])

        return profile_ids

    def getLocalVertices(self, emptyRDD, entities: list):
        schema = StructType([StructField('id', IntegerField(), True),
                             StructField('name'), StringType(), True])

        vertexDF = emptyRDD.toDF(schema)
        node_properties = []
        for entity in entities:
            entity = entity.withColumn("name", F.concat(*node_properties))
            vertexDF = vertexDF.union(entity)

        return vertexDF

    def getLocalEdges(self, df):
        edgesDF = df.select('leftId', 'rightId', 'isMatch').withColumnRenamed("src","leftId")\
            .withColumnRenamed("dst","rightId")\
            .withColumnRenamed("action","isMatch")

        return edgesDF

    def graphObject(self):

        # Initialize spark session
        spark = SparkClass(App_Name)

        # Get all Vertices with appropriate node properties
        emptyRDD = spark.sparkContext.emptyRDD()
        v = getLocalVertices(emptyRDD, entities)

        # Get all edges with appropriate edge for only matches
        mysql = MySQL(config.mysqlLocalHost, App_Name, config.mysqlUser, config.mysqlPassword)
        mysql.mycursor.execute(f'SELECT * FROM {self.table_name};')
        mergesDF = mysql.fetchall()
        e = getLocalEdges(mergesDF)

        # Create a graph
        g = Graphframes(v, e)

        return g

    def getScore(self, entities, df):
        total_profiles = getAllProfiles(entities)
        duplicated_profiles = getAllDuplicate(df)
        duplicates_percentages = duplicated_profiles/ total_profiles
        print(f"Total duplicated identified are, {duplicated_profiles} among {total_profiles} total profiles, i.e. "
              f"{duplicates_percentages}% duplicates.")
        return duplicates_percentages


    # read potential matches from DB [Classifier Output] [leftId <-> rightId <-> 1/0] 1 means match, 0 means not a match
    # Presume output from classifier as
    '''
    *(1000378, 6521361, 0)
    *(1000378, 6521273, 1)
    *(1000378, 6521312, 1)
    *(1000378, 6521334, 0)
    *(1000378, 6521279, 0)
    *(1000378, 6521288, 1)
    *(1000378, 6521286, 1)
    *(1000378, 6521303, 0)
    '''


if __name__ == '__main__':

    # table name from Mysql , Output from classifier
    table_name = ''
    database_name = ''
    g = Mapping(database_name, table_name).graphObject()
    g.cache()

    # Calculate complete pairs : triangle counts
    g.triangleCounts()

    # Calculate all sub_graphs : connected components / strongly connected components
    g.stronglyConnectedComponents()

    # Drop singular un-matched profiles : dropIsolatedVertices
    g.dropIsolatedVertices()