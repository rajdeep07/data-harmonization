import uuid
from typing import Any

from pyspark.sql import SparkSession

from data_harmonization.main.code.tiger.transformer.FloatTransformer import (
    FloatTypeTransformer,
)
from data_harmonization.main.code.tiger.transformer.IntegerTypeTransformer import (
    IntegerTypeTransformer,
)
from data_harmonization.main.code.tiger.transformer.StringTypeTransformer import (
    StringTypeTransformer,
)


class Sanitizer:
    """Cleanse and flatten Entity objects passed to it"""

    def _get_attr_list(self, obj: Any, should_print: bool = False) -> list:
        """Get all the attributes of the object

        Parameters
        ----------
        obj: Any
            Object from which attribute to be returned

        should_print: bool, default=False
            print the list of attributes from the passed object

        Returns
        --------
        list
            list of attributes from the passed object
        """
        if should_print:
            print(obj.get_schema())
        return obj.get_schema()

    def get_kwargs(
        self, cls: dict, attr_lists: dict, gen_id=False, clean_data=False
    ) -> dict:
        """
        Create key arguments from the passed `cls` and `attr_list` and
        assigns unique id to each object

        Parameters
        ----------
        cls: dict
            dict of key value pairs of the object attributes

        attr_lists: dict
            attribute list of the object to be cleansed

        gen_id: bool, default=False
            generate a unique to each passed `cls`

        clean_data: bool, default=False
            Clean the passed `cls` based on the type of attribute from `attr_list`

        Returns
        --------
        dict:
            return raw key word arguments.
        """
        raw_kw = {}
        for attr, tpe in attr_lists.items():
            if attr == "id" and gen_id:
                raw_kw[attr] = uuid.uuid4().hex
            elif tpe in ("int", "str", "float", "int64", "float64", "object"):
                raw_kw[attr] = (
                    self._apply_transformer(cls.get(attr, ""))
                    if clean_data
                    else cls.get(attr, "")
                )  # for dict conversion
            else:
                raw_kw[attr] = ""

        return raw_kw

    def toRawEntity(
        self,
        data: dict,
        rawentity_obj,
        clean_data: bool = True,
        return_dict: bool = True,
    ):
        """Converts passed dict into RawEntity object

        Parameters
        ----------
        data: dict
            dictionary of key value to be converted into RawEntity object

        rawentity_obj: Rawentity, default=RawEntity

        clean_data: bool, defaults=True
            clean `data` before creating `Rawentity` object

        return_dict: bool, defaults=True
            return dict if True else Rawentity object

        Returns
        ---------
        Rawentity, dict
            returns Rawentity if `return_dict` False else dict
        """

        raw_attribute_lists = self._get_attr_list(rawentity_obj)
        raw_kw = self.get_kwargs(
            data,
            attr_lists=raw_attribute_lists,
            clean_data=clean_data,
            gen_id=False,
        )
        raw_entity_object = rawentity_obj(**raw_kw)
        if return_dict:
            return raw_kw
        return raw_entity_object

    def toEntity(
        self, Ent_Obj: Any, data: dict, gen_id=True, return_dict: bool = True
    ):
        """Converts passed dict into the type of `Ent_obj`

        Parameters
        ----------
        data: dict
            dictionary of key value to be converted into RawEntity object

        Ent_Obj: Any
            Enityt object that the data need to be converted

        clean_data: bool, defaults=True
            clean `data` before creating `Rawentity` object

        return_dict: bool, defaults=True
            return dict if True else Rawentity object

        Returns
        ---------
        Ent_Obj, dict
            returns Rawentity if `return_dict` False else dict
        """
        entity_attr_list = self._get_attr_list(Ent_Obj)
        kw = self.get_kwargs(data, entity_attr_list, gen_id)
        entity_obj = Ent_Obj(**kw)
        if return_dict:
            return kw
        return entity_obj

    def _apply_transformer(self, value: str or int or float) -> str:
        """
        Apply Tranformer based on the type of value

        Parameters
        ---------
        value: str or int or float
            apply transformers and cleanse the value based on the type of `value`

        Returns
        --------
        str:
            returns cleansed value as a string
        """
        transformed_ = ""
        if str(value).isdigit() or isinstance(value, int):
            transformed_ = IntegerTypeTransformer.standardizeIntegerType(
                str(value)
            )
        elif isinstance(value, str):
            transformed_ = StringTypeTransformer.standardizeStringType(value)
        elif isinstance(value, float):
            transformed_ = FloatTypeTransformer.standardizeFloatType(
                str(value)
            )
            # transformed_ = value if value is not None else ""
        return transformed_


if __name__ == "__main__":
    from data_harmonization.main.code.tiger.model.ingester import *

    entity1 = Pbna(
        id=12,
        Name="ABC",
        City="Kalyani",
        State="WB",
        Zip=741250,
        Address="Bedibhawan",
        source="src",
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
            CISID="",
        )
    ]
    snt = Sanitizer()
    spark = SparkSession.builder.appName("sanitizer").getOrCreate()
    df = spark.createDataFrame(test)
    print(df.head())
    print("Entity:\n", snt.toEntity(Pbna, df.head().asDict()))
    print("RawEntity:\n", snt.toRawEntity(df.head().asDict(), clean_data=True))
