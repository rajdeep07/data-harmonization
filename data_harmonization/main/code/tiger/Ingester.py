from data_harmonization.main.code.tiger.database.SchemaGenerator import SchemaGenerator
import os
from os import listdir
import sys, importlib
import inspect
import numpy as np
from collections import Counter
from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
import os
import sys
from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass
import netifaces
import data_harmonization.main.resources.config as config


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
App_Name = "data_harmonization"
database_name = "data_harmonization"

class RawEntity:
    def __init__(self):
        pass


class Ingester(RawEntity, Sanitizer):

    current_dir = os.path.dirname(os.path.realpath(__file__))
    target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-2])

    # Step 0: Read individual uploaded CSVs and Infer Schema
    filenames = listdir(target_dir + "/data/")
    csv_filenames = [
        filename
        for filename in filenames
        if filename.endswith(".csv") and not filename.startswith("benchmark")
    ]

    schema_dir = str(target_dir + '/code/tiger/model/Ingester/')

    for csv_file in csv_filenames:
        schemaGen = SchemaGenerator(str(target_dir + '/data/' + csv_file), schema_dir)
        schemaGen.generate_class()

    # Step 1: read individual csvs and write to mysql with same referred class

    # get all entities
    class_names = [
        class_name
        for class_name in listdir(schema_dir)
        if class_name.endswith(".py") and not class_name.startswith("__init__") and not class_name.startswith("Bottom")
    ]
    print(class_names)
    # Find common attributes for raw entity class
    total_attributes = []
    for name, cls in inspect.getmembers(importlib.import_module("data_harmonization.main.code.tiger.model.ingester"),
                                        inspect.isclass):
        total_attributes.append([i for i in cls.__dict__.keys() if i[:1] != '_'])

    total_attributes = Counter(list(np.concatenate(total_attributes).flat))

    raw_entity_attrs = list()
    for key, value in total_attributes.items():
        if value == len(class_names):
            raw_entity_attrs.append(key)

    # Step 2: Create Raw Entity class with appropriate attribute
    for x in range(len(raw_entity_attrs)):
        setattr(RawEntity(), raw_entity_attrs[x], x)

    # Initialize spark session
    spark = SparkClass(App_Name)
    spark.init_db(config.mysqlUser, config.mysqlPassword, config.mysqlLocalHost)

    # Step 3: Write individual entities to MySQL
    for csv_file in csv_filenames:
        # TODO: generalize csv reader to almost everything later.
        df = spark.readFromCSV(str(target_dir + '/data/' + csv_file))
        spark.writeToDatabase(db=database_name, table=csv_file.split(".")[:-1], df=df, mode='overwrite')

    # Step 4: With Raw Entities ==> apply sanitiser ==> Persist in MySQL
    ## Sanitiser ==> dictionary spark.dataFrames()
    sanitiser = Sanitizer()
    # sanitiser.toRawEntity()

    # Step 4: Write to MySQL RawEntity


if __name__ == '__main__':
    Ingester()
