import os
import sys
from data_harmonization.main.code.tiger.database.MySQL import MySQL
from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass
import netifaces
import data_harmonization.main.resources.config as config

### TODO: Example code for database connections and spark functionalities
### TODO: Since Clustering and Classifier doesn't currently support pyspark dataframe, pls use toPandas() method.

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
App_Name = "data_harmonization"


def get_data_from_db(table_name):
    mysql = MySQL(config.mysqlLocalHost, App_Name, config.mysqlUser, config.mysqlPassword)
    mysql.mycursor.execute(f'SELECT * FROM {table_name};')
    return mysql.fetchall()

def write_csv_to_db(file_path, database_name, table_name):
    spark = SparkClass(App_Name)
    spark.init_db(config.mysqlUser, config.mysqlPassword, config.mysqlLocalHost)
    df = spark.readFromCSV(file_path)
    spark.writeToDatabase(db=database_name, table=table_name, df=df, mode='overwrite')




