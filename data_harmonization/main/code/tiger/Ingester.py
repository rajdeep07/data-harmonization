from pathlib import Path
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
import data_harmonization.main.resources.config as config_
from pyspark.sql import Row
import pyspark.sql.functions as F

# from data_harmonization.main.code.tiger.model.ingester import *
from data_harmonization.main.code.tiger.database.MySQL import MySQL
from data_harmonization.main.code.tiger.model.ingester.raw_entity import RawEntity


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
App_Name = "data_harmonization"
database_name = "data_harmonization"
raw_entity_table_name = "Raw_Entity"


class Ingester:
    def ingester(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-2])

        # Step 0: Read individual uploaded CSVs and Infer Schema
        filenames = listdir(target_dir + "/data/")
        csv_filenames = [
            filename
            for filename in filenames
            if filename.endswith(".csv") and not filename.startswith("benchmark")
        ]

        schema_dir = str(target_dir + "/code/tiger/model/ingester/")

        for csv_file in csv_filenames:
            schemaGen = SchemaGenerator(
                str(target_dir + "/data/" + csv_file), schema_dir
            )
            schemaGen.generate_class()

        # Step 1: read individual csvs and write to mysql with same referred class

        # get all entities
        class_names = [
            class_name
            for class_name in listdir(schema_dir)
            if class_name.endswith(".py")
            and not class_name.startswith("__init__")
            and not class_name.startswith("Bottom")
        ]
        print(class_names)
        # Find common attributes for raw entity class
        total_attributes = []
        generated_classes = {}
        for name, cls in inspect.getmembers(
            importlib.import_module(
                "data_harmonization.main.code.tiger.model.ingester"
            ),
            inspect.isclass,
        ):
            total_attributes.append([i for i in cls.__dict__.keys() if i[:1] != "_"])
            generated_classes[name] = cls

        total_attributes = Counter(list(np.concatenate(total_attributes).flat))

        raw_entity_attrs = list()
        for key, value in total_attributes.items():
            if value == len(class_names):
                raw_entity_attrs.append(key)

        # Step 2: Create Raw Entity class with appropriate attribute
        for x in range(len(raw_entity_attrs)):
            setattr(RawEntity(), raw_entity_attrs[x], x)

        # Initialize spark session
        spark = SparkClass()
        # spark.init_db(config_.mysqlUser, config_.mysqlPassword, config_.mysqlLocalHost)

        # Step 3: Write individual entities to MySQL
        sanitiser = Sanitizer()
        db = MySQL(
            config_.mysqlLocalHost,
            config_.DB_NAME,
            config_.mysqlUser,
            config_.mysqlPassword,
        ).SessionMaker()
        data_rows = []
        raw_entity_list = []
        entity_list = []
        final_df = None
        for csv_file in csv_filenames:
            # TODO: generalize csv reader to almost everything later.
            df = spark.read_from_csv_to_dataframe(str(target_dir + "/data/" + csv_file))
            df = self.drop_null_columns(df)

            # save sanitized dataframe into sql database
            df.show()
            # class_name = Path(csv_file).stem.capitalize()
            # print(class_name)

            # data_collect = df.collect()
            # for k, v in generated_classes.items():
            #     if k.__eq__(class_name):
            #         for data in data_collect:
            #             if v is None:
            #                 print(v)
            #             class_entity = sanitiser.toEntity(v, data, True)
            #             db.add(class_entity)
            #             entity_dict = class_entity.get_values()

            #             semi_flatten_profile = {}
            #             for k_entity, v_entity in entity_dict.items():
            #                 # print(list(v))
            #                 # a = list(v)
            #                 # v = list(v)[0] if v else None
            #                 if not isinstance(v_entity, (int, str, float)) and v_entity:
            #                     for k1, v1 in v_entity.__dict__.items():
            #                         semi_flatten_profile[k1] = v1
            #                 else:
            #                     semi_flatten_profile[k_entity] = v_entity
            #             entity_list.append(semi_flatten_profile)
            # raw_entity = sanitiser.toRawEntity(
            #     entity_dict, gen_id=True, clean_data=True
            # )
            # semi_flatten_rawprofile = {}
            # raw_dict = raw_entity.__dict__
            # for k, v in raw_dict.items():
            #     if not isinstance(v, (int, str, float)) and v:
            #         for k1, v1 in v.__dict__.items():
            #             semi_flatten_rawprofile[k1] = v1
            #     else:
            #         semi_flatten_rawprofile[k] = v
            # raw_entity_list.append(semi_flatten_rawprofile)
            #         break

            # db.commit()

            # # create pyspark dataframe for current csv file
            # entity_df = spark.spark.createDataFrame(entity_list)
            # entity_df.show()
            # entity_df_collect = entity_df.collect()
            # for data in entity_df_collect:
            #     raw_entity = sanitiser.toRawEntity(data, gen_id=True, clean_data=True)
            #     print(raw_entity)

            # save raw data from csv in sql dataframe
            spark.write_to_database_from_df(
                db=database_name,
                table="".join(csv_file.split(".")[:-1]),
                df=df,
                mode="overwrite",
            )
            final_df = df
        # db.close()
        # raw_entities = spark.spark.createDataFrame(raw_entity_list)
        # raw_entities.show()

        # Step 4: With Raw Entities ==> apply sanitiser ==> Persist in MySQL
        ## Sanitiser ==> dictionary spark.dataFrames()

        output = final_df.rdd.map(
            lambda r: sanitiser.toRawEntity(r, gen_id=True, clean_data=True)
        ).toDF()
        # data_collect = final_df.collect()
        # raw_entity_list = []
        # # raw_entities = spark.spark.sparkContext.emptyRDD()
        # for data in data_collect:
        #     raw_entity = sanitiser.toRawEntity(data, gen_id=True, clean_data=True)
        #     semi_flatten_rawprofile = {}
        #     raw_dict = raw_entity.__dict__
        #     for k, v in raw_dict.items():
        #         if not isinstance(v, (int, str, float)) and v:
        #             for k1, v1 in v.__dict__.items():
        #                 semi_flatten_rawprofile[k1] = v1
        #         else:
        #             semi_flatten_rawprofile[k] = v
        #     raw_entity_list.append(semi_flatten_rawprofile)
        # raw_entities = spark.spark.createDataFrame(raw_entity_list)
        # raw_entities.show()

        # Step 4: Write to MySQL RawEntity
        spark.write_to_database_from_df(
            db=database_name,
            table=raw_entity_table_name,
            df=output,
            mode="overwrite",
        )

    def drop_null_columns(self, df, threshold=-1):
        """
        This function drops all columns which contain null values.
        If threshold is negative (default), drop columns that have only null values.
        If threshold is >=0, drop columns that have count of null values bigger than threshold. This may be very computationally expensive!
        Returns PySpark DataFrame.
        """
        if threshold < 0:
            max_per_column = (
                df.select([F.max(c).alias(c) for c in df.columns]).collect()[0].asDict()
            )
            to_drop = [k for k, v in max_per_column.items() if v == None]
        else:
            null_counts = (
                df.select(
                    [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
                )
                .collect()[0]
                .asDict()
            )
            to_drop = [k for k, v in null_counts.items() if v > threshold]
        df = df.drop(*to_drop)
        return df


if __name__ == "__main__":
    Ingester().ingester()
