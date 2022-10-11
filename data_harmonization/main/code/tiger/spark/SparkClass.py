from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark import SparkConf, SparkFiles
from pyspark.context import SparkContext
import urllib.request
from pyspark.sql import functions as F
import data_harmonization.main.resources.config as config_
import findspark
# import MySQLdb


class SparkClass:

    # Step 1: We all need a spark session instance
    def __init__(self) -> None:
        # establish these as SPARK_HOME and PYTHON_HOME, with PATHS in your zshrc or bashrc
        findspark.init("/home/navazdeen/spark-3.1.1-bin-hadoop3.2", "/home/navazdeen/miniconda3/envs/data-harmonization/bin/python")
        # add this to external jars and pass when initializing spark session
        findspark.add_packages('mysql:mysql-connector-java:8.0.11')

        self.spark = SparkSession.builder.master("local[*]").appName(config_.APP_NAME)\
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

    # def get_mysql_cursor(self):
    #     connection = MySQLdb.connect(host=config_.mysqlLocalHost,
    #                                  user=config_.mysqlUser,
    #                                  passwd=config_.mysqlPassword,
    #                                  db=config_.DB_NAME)
    #     cursor = connection.cursor()
    #     cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = {config_.DB_NAME}")
    #     return cursor

    # Step 2: read_from_database_to_dataframe [MySQL]
    def read_from_database_to_dataframe(self, table, columnTypes=None) -> DataFrame:
        df = self.spark.read.format('jdbc').options(
                url=f'jdbc:mysql://localhost/{config_.APP_NAME}',
                driver=config_.mysqlDriver,
                dbtable=table,
                user=config_.mysqlUser,
                password=config_.mysqlPassword,
                autoReconnect=True,
                useSSL=False,
                verifyServerCertificate=False).load()

        if columnTypes:
            for col in columnTypes:
                df = df.withColumn(col, F.col(col).cast(columnTypes[col]))

        return df

    # Step 3: read_from_csv_to_dataframe
    def read_from_csv_to_dataframe(self, csv_file_path, header=True, inferSchema=True) -> DataFrame:
        return self.spark.read.csv(csv_file_path, header=header, inferSchema=inferSchema)


    # Step 4: write_to_csv_from_df
    # data is distributed in 4 partitions: reduce or pandas
    def write_to_csv_from_df(self, local_path, df) -> None:
        return df.repartition(1).write.format('com.databricks.spark.csv').save(local_path, header='true')
        # df.toPandas().to_csv(local_path)

    # Step 5: write_to_database_from_df
    # data is distributed in 4 partitions: reduce or pandas [MySQL]
    # MySQL [RDBMS] ==> NoSQL or Document DB [Cassandra/ ES/ anything..]
    def write_to_database_from_df(self, table, df, mode='error') -> None:
        df.write.format('jdbc').options(
            url=f'jdbc:mysql://localhost/{config_.APP_NAME}',
            driver=config_.mysqlDriver,  # 'com.mysql.cj.jdbc.Driver',
            dbtable=table,
            user=config_.mysqlUser,
            password=config_.mysqlPassword,
            autoReconnect=True,
            useSSL=False,
            verifyServerCertificate=False).mode(mode).save()

    def get_sparkSession(self):
        return self.spark

if __name__ == "__main__":
    SparkClass()

