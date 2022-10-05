from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark import SparkConf, SparkFiles
from pyspark.context import SparkContext
import urllib.request
import data_harmonization.main.resources.config as config


class SparkClass:
    def __init__(self, appName, config=None):
        self.appName = appName
        self.user = ''
        self.password = ''

        if config:
            config_ = SparkConf().setAll(config)
            self.spark = SparkSession().appName("data_harmonization").config(conf=config_).getorCreate()
        else:
            self.spark = SparkSession().appName("data_harmonization").getorCreate()

    def init_db(self, user, password, host):
        self.user = user
        self.password = password
        self.host = host
        self.driver = 'com.mysql.cj.jdbc.Driver'

    def writeToDatabase(self, db, table, df, mode='error'):
        df.write.format('jdbc').options(
            url=f'jdbc:mysql://{self.host}/{db}',
            driver=self.driver,  # 'com.mysql.cj.jdbc.Driver',
            dbtable=table,
            user=self.user,
            password=self.password).mode(mode).save()

    def readFromDatabase(self, db, table):
        df = self.spark.read.format('jdbc').options(
                url=f'jdbc:mysql://{self.host}/{db}',
                driver=self.driver,
                dbtable=table,
                user=self.user,
                password=self.password).load()
        return df

    def readFromCSV(self, csv_file_path, header=True, inferSchema=True):
        return self.spark.read.csv(csv_file_path, header=header, inferSchema=inferSchema)

    def writeToCSV(self, csv_path, df):
        df.repartition(1).write.format('com.databricks.spark.csv').save(csv_path, header='true')
