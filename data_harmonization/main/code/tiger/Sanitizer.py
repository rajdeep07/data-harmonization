import uuid
from typing import Any

import pandas as pd

# from data_harmonization.main.code.tiger.model.dataclass import RawEntity, CleansedRawEntity
from data_harmonization.main.code.tiger.model.ingester import *
from data_harmonization.main.code.tiger.transformer.IntegerTypeTransformer import \
    IntegerTypeTransformer
from data_harmonization.main.code.tiger.transformer.StringTypeTransformer import \
    StringTypeTransformer
from pyspark.sql import SparkSession


class Sanitizer:
    def _get_attr_list(self, obj : Any, should_print : bool = False) -> list:
        if should_print:
            print(obj.get_schema())
        return obj.get_schema()

    def get_kwargs(self, cls:dict, attr_lists : dict, gen_id=False, clean_data=False):
        raw_kw = {}
        for attr, tpe in attr_lists.items():
            if attr == "id" and gen_id:
                raw_kw[attr] = uuid.uuid4().hex
            elif tpe in ("int", "str", "float", "int64", "float64", "object"):
                raw_kw[attr] = self._apply_transformer(cls.get(attr, ""))  if clean_data else cls.get(attr, "")  # for dict conversion
            else:
                raw_kw[attr] = ''
            
        return raw_kw

    def toRawEntity(self, cls : Any, clean_data:bool = False, gen_id:bool=False) -> Rawentity:
        raw_attribute_lists = list(self._get_attr_list(Rawentity))[0]
        raw_kw = self.get_kwargs(cls, attr_lists=raw_attribute_lists, clean_data=clean_data, gen_id=gen_id)
        raw_entity_object = Rawentity(**raw_kw)
        return raw_entity_object
    
    def toEntity(self, Ent_Obj : Any, data:dict, gen_id=True, return_dict:bool=True):
        # print(data)
        entity_attr_list = self._get_attr_list(Ent_Obj)
        kw = self.get_kwargs(data, entity_attr_list, gen_id)
        entity_obj = Ent_Obj(**kw)
        if return_dict: return kw
        return entity_obj

    def _apply_transformer(self, value:str or int):
        if value.isdigit() or isinstance(int, value):
            transformed_ = IntegerTypeTransformer.standardizeIntegerType(str(value))
        elif isinstance(str, value):
            transformed_ = StringTypeTransformer.standardizeStringType(value)
        return transformed_


if __name__ == "__main__":
    entity1 = Pbna(
        id=12,
        Name="ABC",
        City="Kalyani",
        State="WB",
        Zip=741250,
        Address="Bedibhawan",
        source="src"
    )
    test = [
        dict(
            Name="tiger Analytics",
            City="new_city",
            Zip=123456,
            Address="231, asdf, asdfgh",
            State="ca",
            gender="M",
            source="XYZ",
            age=25,
            CISID=""
        )
    ]
    snt = Sanitizer()
    spark = SparkSession.builder.appName("sanitizer").getOrCreate()
    df = spark.createDataFrame(test)
    print(df[0])
    print(entity1)  
    print("Entity:\n", snt.toEntity(Pbna, df[0]))
    print("RawEntity:\n", snt.toRawEntity(entity1, clean_data=True))