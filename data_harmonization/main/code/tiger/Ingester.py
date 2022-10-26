import importlib
import inspect
import os
from collections import Counter
from functools import reduce
from os import listdir
from pathlib import Path

import data_harmonization.main.resources.config as config
from data_harmonization.main.code.tiger.database import MySQL
from data_harmonization.main.code.tiger.database.SchemaGenerator import SchemaGenerator
from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
from data_harmonization.main.code.tiger.spark import SparkClass
from pyspark.sql import DataFrame


class Ingester:
    """Read Csv file from the target folder and trigger schema generator
    to generate schemas also persist raw data and raw entity into MySQL database."""

    def __init__(self):
        self.spark = SparkClass()
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.target_dir = os.path.sep.join(self.current_dir.split(os.path.sep)[:-2])
        self.csv_files = self._get_csv_files()
        self.schema_dirs = self._get_schema_dirs()

    # Step 0: Read individual uploaded CSVs and Infer Schema
    def _get_csv_files(self) -> list[str]:
        """return all the csv file name from the target directory as a list

        Returns
        --------
        list
            csv file name from the target directory
        """
        filenames = listdir(self.target_dir + "/data/")
        return [
            filename
            for filename in filenames
            if filename.endswith(".csv") and not filename.startswith("benchmark")
        ]

    def _get_schema_dirs(self) -> str:
        """return the absolute path of schema directory

        Returns
        ---------
        str
            Absolute path of schema directory
        """
        return str(self.target_dir + "/code/tiger/model/ingester/")

    def _generate_schemas(self) -> None:
        """Generate schema's in the schema directory for all the csv filenames"""
        for csv_file in self.csv_files:
            SchemaGenerator(
                str(self.target_dir + "/data/" + csv_file), self.schema_dirs
            ).generate_class()

    def _get_all_tables(self) -> list[str]:
        """return all the schema table names

        Returns
        --------
        list
            list of generated schema names
        """
        return MySQL.get_tables()

    # Find common attributes for raw entity class
    def _gen_raw_entity(self, features_for_deduplication: list = None) -> None:
        """Generate Raw entity Schema in the schema directory

        Parameters
        -----------
        features_for_deduplication : list, optional, default=None
                                     list of columns from the data source to be used
                                     for deduplication

        Returns
        ---------
        None
        """
        total_attributes = []
        attr_dict = dict()
        for _, cls in inspect.getmembers(
            importlib.import_module("data_harmonization.main.code.tiger.model.ingester"),
            inspect.isclass,
        ):
            total_attributes.extend(cls.get_schema().keys())
            attr_dict.update(cls.get_schema())

        total_attributes_count = Counter(total_attributes)

        raw_entity_attrs = dict()
        table_list = self._get_all_tables()
        if features_for_deduplication:
            total_attributes_count = {
                key: total_attributes_count[key] for key in features_for_deduplication
            }

        for key, value in total_attributes_count.items():
            if value == len(table_list):
                raw_entity_attrs[key] = attr_dict[key]

        raw_entity_attrs.pop("id")
        SchemaGenerator().generate_class_from_schema(
            raw_entity_attrs, config.raw_entity_table, self.schema_dirs
        )

    def _persist_csv_to_mysql(self, path=None) -> None:
        """Persist csv data to MySQL database

        Parameters
        ----------
        path : str, Optional
             path for the target directory

        Returns
        ---------
        None
        """
        generated_classes = {}
        for csv_file in self.csv_files:
            # TODO: generalize csv reader to almost everything later.
            sanitiser = Sanitizer()
            df = self.spark.read_from_csv_to_dataframe(str(self.target_dir + "/data/" + csv_file))
            class_name = Path(csv_file).stem.capitalize()

            class_ = None
            # get all classes
            for name, cls in inspect.getmembers(
                importlib.import_module("data_harmonization.main.code.tiger.model.ingester"),
                inspect.isclass,
            ):
                generated_classes[name] = cls
            # taking classes dynamically and passing it to sanitizer
            for k, v in generated_classes.items():
                if k.__eq__(class_name):
                    class_ = v
                    break
            ls = df.rdd.map(lambda row: sanitiser.toEntity(class_, row.asDict())).toDF(
                sampleRatio=0.01
            )
            self.spark.write_to_database_from_df(
                table=csv_file.split(".")[0], df=ls, mode="overwrite"
            )

    def _persist_raw_entity(self, features_for_deduplication=None) -> None:
        """
        Parameters
        ----------
        features_for_deduplication : list
            list of features to be used for `deduplication`

        Returns
        --------
        None
        """

        # cursor = self.spark.get_mysql_cursor()
        importlib.invalidate_caches()
        rawentity = importlib.import_module(
            "data_harmonization.main.code.tiger.model.ingester.Rawentity"
        )
        series = []
        table_names = self._get_all_tables()
        if config.raw_entity_table in table_names:
            table_names.remove(config.raw_entity_table)

        # PBNA + FLNA ==> concat[vertical stack] ==> RawEntity
        for table in table_names:
            df_ = self.spark.read_from_database_to_dataframe(table)
            series.append(df_)

        df_series = reduce(DataFrame.unionAll, series)
        # ls = df_series.rdd.map(lambda row : Sanitizer().toRawEntity(row.asDict())).collect()

        df_series = df_series.rdd.map(
            lambda r: Sanitizer().toRawEntity(data=r.asDict(), rawentity_obj=rawentity.Rawentity)
        ).toDF()

        if features_for_deduplication:
            df_series = df_series.select(features_for_deduplication)

        self.spark.write_to_database_from_df(
            table=config.raw_entity_table, df=df_series, mode="overwrite"
        )

        return


if __name__ == "__main__":
    ingester = Ingester()
    ingester._generate_schemas()
    ingester._gen_raw_entity()
    ingester._persist_csv_to_mysql()
    ingester._persist_raw_entity()
