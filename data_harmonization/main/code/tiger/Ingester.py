from data_harmonization.main.code.tiger.database.SchemaGenerator import SchemaGenerator
from data_harmonization.main.code.tiger.database import MySQL
import os
from os import listdir
import sys, importlib
import inspect
import numpy as np
from collections import Counter
import os
import sys
from data_harmonization.main.code.tiger.spark import SparkClass
import data_harmonization.main.resources.config as config
from data_harmonization.main.code.tiger.model.ingester import *
from pyspark.sql import DataFrame
from functools import reduce



os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
App_Name = "data_harmonization"
database_name = "data_harmonization"



class Ingester():

    def __init__(self):
        self.spark = SparkClass()
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
    def _get_all_tables(self) -> list[str]:
        return MySQL.get_tables()

    # TODO: This is not a generalize method, it presumes only reading from csv before writing to mysql.
    # TODO: Also only meant for raw files upload.
    def _persist_csv_to_mysql(self, path=None): # write_to_mysql
        from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
        for csv_file in self.csv_files:
            # TODO: generalize csv reader to almost everything later.
            sanitiser = Sanitizer()
            df = self.spark.read_from_csv_to_dataframe(str(self.target_dir + '/data/' + csv_file))
            ls = df.rdd.map(lambda row : sanitiser.toEntity(Pbna, row.asDict())).toDF(sampleRatio=0.01)
            self.spark.write_to_database_from_df(table=csv_file.split(".")[0], df=ls,
                                                 mode='overwrite')

    # Find common attributes for raw entity class
    def _gen_raw_entity(self, features_for_deduplication=None):
        total_attributes = []
        attr_dict = dict()
        for _, cls in inspect.getmembers(importlib.import_module("data_harmonization.main.code.tiger.model.ingester"),
                                            inspect.isclass):
            total_attributes.extend(cls.get_schema().keys())
            attr_dict.update(cls.get_schema())    

        total_attributes_count = Counter(total_attributes)

        raw_entity_attrs = dict()
        table_list = self._get_all_tables()
        for key, value in total_attributes_count.items():
            if value == len(table_list):
                raw_entity_attrs[key] = attr_dict[key]

        raw_entity_attrs.pop('id')
        # Step 2: Create Raw Entity class with appropriate attribute
        # for x in range(len(raw_entity_attrs)):
        #     setattr(RawEntity(), raw_entity_attrs[x], x)
        SchemaGenerator().generate_class_from_schema(raw_entity_attrs, 'RawEntity', self.schema_dirs)

    def _persist_raw_entity(self, features_for_deduplication=None):
        '''
        :param features_for_deduplication: User []
        :return:
        '''

        # cursor = self.spark.get_mysql_cursor()
        from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
        series = []
        table_names = self._get_all_tables()
        table_names.remove('Rawentity')

        # PBNA + FLNA ==> concat[vertical stack] ==> RawEntity
        for table in table_names:
            df_ = self.spark.read_from_database_to_dataframe(table.lower())
            series.append(df_)

        df_series = reduce(DataFrame.unionAll, series)
        # ls = df_series.rdd.map(lambda row : Sanitizer().toRawEntity(row.asDict())).collect()

        df_series = df_series.rdd.map(lambda r : Sanitizer().toRawEntity(r.asDict())).toDF()

        if features_for_deduplication:
            df_series = df_series.select(features_for_deduplication)

        self.spark.write_to_database_from_df(table="rawentity", df=df_series, mode='overwrite')

        return

        # Step 4: With Raw Entities ==> apply sanitiser ==> Persist in MySQL
        ## Sanitiser ==> dictionary spark.dataFrames()
        # sanitiser.toRawEntity()

        # Step 4: Write to MySQL RawEntity


if __name__ == '__main__':
    ingester = Ingester()
    ingester._generate_schemas()
    ingester._gen_raw_entity()
    ingester._persist_csv_to_mysql()
    ingester._persist_raw_entity()

# TODO: sort methods, fix broken imports
