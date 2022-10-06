from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark import SparkConf, SparkFiles
from pyspark.context import SparkContext
import urllib.request
import data_harmonization.main.resources.config as config
from pyspark.sql import functions as F
import data_harmonization.main.resources.config as config_

class SparkClass:

    # Step 1: We all need a spark session instance
    spark = SparkSession.builder().master("local[*]").appName(config_.APP_NAME)\
        .config("spark.sql.shuffle.partitions", "2").getOrCreate()

    # Step 2: read_from_database_to_dataframe [MySQL]
    def read_from_database_to_dataframe(self, spark, table, columnTypes=None):
        df = spark.read.format('jdbc').options(
                url=f'jdbc:mysql://{config_.mysqlLocalHost}/{config_.DB_NAME}',
                driver=config_.mysqlDriver,
                dbtable=table,
                user=config_.mysqlUser,
                password=config_.mysqlPassword).load()

        if columnTypes:
            for col in columnTypes:
                df = df.withColumn(col, F.col(col).cast(columnTypes[col]))

        return df

    # Step 3: read_from_csv_to_dataframe
    def read_from_csv_to_dataframe(self, spark, csv_file_path, header=True,  inferSchema=True):
        return spark.read.csv(csv_file_path, header=header, inferSchema=inferSchema)


    # Step 4: write_to_csv_from_df
    # data is distributed in 4 partitions: reduce or pandas
    def write_to_csv_from_df(self, local_path, df):
        return df.repartition(1).write.format('com.databricks.spark.csv').save(local_path, header='true')
        # df.toPandas().to_csv(local_path)

    # Step 5: write_to_database_from_df
    # data is distributed in 4 partitions: reduce or pandas [MySQL]
    # MySQL [RDBMS] ==> NoSQL or Document DB [Cassandra/ ES/ anything..]
    def write_to_database_from_df(self, db, table, df, mode='error'):
        df.write.format('jdbc').options(
            url=f'jdbc:mysql://{config_.mysqlLocalHost}/{config_.APP_NAME}',
            driver=config_.mysqlDriver,  # 'com.mysql.cj.jdbc.Driver',
            dbtable=table,
            user=config_.mysqlUser,
            password=config_.mysqlPassword).mode(mode).save()

if __name__ == "__main__":
    SparkClass()

