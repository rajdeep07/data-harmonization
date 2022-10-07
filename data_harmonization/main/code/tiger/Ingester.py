from data_harmonization.main.code.tiger.database.SchemaGenerator import SchemaGenerator
from data_harmonization.main.code.tiger.database import MySQL
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
import data_harmonization.main.resources.config as config


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
App_Name = "data_harmonization"
database_name = "data_harmonization"

class RawEntity:
    def __init__(self):
        pass


class Ingester(SparkClass):

    def __init__(self):
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.target_dir = os.path.sep.join(self.current_dir.split(os.path.sep)[:-2])
        self.csv_files = self._get_csv_files()
        self.schema_dirs = self._get_schema_dirs()
    # Step 0: Read individual uploaded CSVs and Infer Schema
    def _get_csv_files(self):
        filenames = listdir(self.target_dir + "/data/")
        return [
            filename
            for filename in filenames
            if filename.endswith(".csv") and not filename.startswith("benchmark")
        ]

    def _get_schema_dirs(self):
        return str(self.target_dir + '/code/tiger/model/ingester/')

    def _generate_schemas(self):
        for csv_file in self.csv_files:
            SchemaGenerator(str(self.target_dir + '/data/' + csv_file), self.schema_dirs).generate_class()

    # Step 1: read individual csvs and write to mysql with same referred class
    # get all entities
    def _get_all_tables(self) -> list:
        return MySQL.get_tables()

    def _write_to_mysql(self):
        for csv_file in self.csv_files:
            ps = self.read_from_csv_to_dataframe(self.current_dir + "/data" + csv_file)
            ps.foreach(lambda row: Sanitizer().toEntity(1, row))

    # Find common attributes for raw entity class
    def _gen_raw_entity(self):
        total_attributes = []
        attr_dict = dict()
        for _, cls in inspect.getmembers(importlib.import_module("data_harmonization.main.code.tiger.model.ingester"),
                                            inspect.isclass):
            total_attributes.extend(cls.get_schema().keys())
            attr_dict.update(cls.get_schema())    

        total_attributes_count = Counter(total_attributes)

        raw_entity_attrs = list()
        for key, value in total_attributes_count.items():
            if value == len(self._get_all_tables()):
                raw_entity_attrs.append(key)

        # Step 2: Create Raw Entity class with appropriate attribute
        for x in range(len(raw_entity_attrs)):
            setattr(RawEntity(), raw_entity_attrs[x], x)

    # Initialize spark session
    def _init_spark(self):
        spark = SparkClass()
        # spark.init_db(config.mysqlUser, config.mysqlPassword, config.mysqlLocalHost)

        # Step 3: Write individual entities to MySQL
        for csv_file in self.csv_files:
            # TODO: generalize csv reader to almost everything later.
            df = spark.read_from_csv_to_dataframe(str(self.target_dir + '/data/' + csv_file))
            spark.write_to_database_from_df(db=database_name, table=csv_file.split(".")[:-1], df=df, mode='overwrite')

        # Step 4: With Raw Entities ==> apply sanitiser ==> Persist in MySQL
        ## Sanitiser ==> dictionary spark.dataFrames()
        sanitiser = Sanitizer()
        # sanitiser.toRawEntity()

        # Step 4: Write to MySQL RawEntity


if __name__ == '__main__':
    ingester = Ingester()
    ingester._generate_schemas()
    ingester._init_spark()
    ingester._write_to_mysql()
    ingester._gen_raw_entity()
