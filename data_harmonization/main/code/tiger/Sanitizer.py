from data_harmonization.main.code.tiger.transformer.CityTransformer import *
from data_harmonization.main.code.tiger.transformer.NameTransformer import *
from data_harmonization.main.code.tiger.transformer.PostalAddressTransformer import *
from data_harmonization.main.code.tiger.transformer.StateTransformer import *
from data_harmonization.main.code.tiger.transformer.ZipCodeTransformer import *
from data_harmonization.main.code.tiger.transformer.IntegerTypeTransformer import *
from data_harmonization.main.code.tiger.transformer.StringTypeTransformer import *
from data_harmonization.main.code.tiger.model import GeocodedAddress, PostalAddress
#from data_harmonization.main.code.tiger.model.dataclass import RawEntity, CleansedRawEntity
from data_harmonization.main.code.tiger.model.datamodel import *
from dataclasses import dataclass
from data_harmonization.main.code.tiger.model.datamodel import *
from typing import Any
import pandas as pd


class Sanitizer():
    def _get_attr_list(self, obj:Any, should_print=False) -> dict:
        items = obj.__dict__.items()
        if should_print:
            [print(f"attribute: {k}  value: {v}") for k, v in items if k in ("__annotations__")]
        return(value for item, value in items if item in ("__annotations__"))

    def get_kwargs(self, df_obj:pd.DataFrame, attr_lists:list) -> dict:
        self.cls_map = {}
        for attr, dtype in attr_lists.items():
            dtype = dtype.replace("Optional[","").replace("]","")
            self.cls_map[attr] = globals().get(dtype, dtype)
        raw_kw = {}
        for attr, cls_type in self.cls_map.items():
            kw = {}
            if cls_type not in ('int', 'str', 'float'):
                sub_attr_list = list(self._get_attr_list(self.cls_map[attr]))[0]
                for sub_attr in sub_attr_list.keys():
                        kw[sub_attr] = self._apply_transformer(cls_type(), df_obj.get(sub_attr))
                raw_kw[attr] = self.cls_map[attr](**kw)
            else:
                raw_kw[attr] = self._apply_transformer(df_obj.get(attr), df_obj.get(attr))
        return raw_kw

    def toRawEntity(self, df_obj:pd.DataFrame) -> RawEntity:

        raw_attribute_lists = list(self._get_attr_list(RawEntity))[0]
        raw_kw = self.get_kwargs(df_obj, raw_attribute_lists)
        raw_entity_object = RawEntity(**raw_kw)
        return raw_entity_object

    def _apply_transformer(self, cls_type, attr):
        if not isinstance(cls_type, int) and not isinstance(cls_type, str):
            if isinstance(cls_type, Name):
                transformed_ = NameTransformer.standardizeName(attr)
            elif isinstance(cls_type, Address):
                transformed_ = PostalAddressTransformer.standardizePostalAddress(attr)
            elif isinstance(cls_type, Gender):
                # transformed_ = standardizeGender(attr)
                pass
            elif isinstance(cls_type, Email):
                # transformed_ = standardizeEmail(attr)
                pass
            elif isinstance(cls_type, Contact):
                # transformed_ =  standardizeContact(attr)
                pass
            elif isinstance(cls_type, int):
                transformed_ = IntegerTypeTransformer.standardizeIntegerType(attr)
            elif isinstance(cls_type, str):
                transformed_ = StringTypeTransformer.standardizeStringType(attr)
            elif cls_type is None:
                transformed_ = cls_type
            elif isinstance(cls_type, list):
                transformed_list = []
                for item in attr:
                    transformed_list.append(self._apply_transformer(type(item), item))
        elif isinstance(cls_type, int):
            transformed_ = IntegerTypeTransformer.standardizeIntegerType(str(cls_type))
        elif isinstance(cls_type, str):
            transformed_ = StringTypeTransformer.standardizeStringType(cls_type)
        elif cls_type is None:
            transformed_ = attr
        return transformed_


if __name__ == "__main__":
    addr = Address(
        city="Kolkata!22##*!?@34",
        zipcode=700000,
        address="Saltlake"
    )
    entity1 = Entity1(
        id=12,
        name="ABC",
        city="Kalyani",
        state="WB",
        zipcode=741250,
        addr="Bedibhawan",
        source="src",
        gender='F'
    )
    test = pd.Series(
        dict(Name="tiger Analytics",
        cluster_id=1,
        City = "new_city",
        Zip = 123456,
        Address = "231, asdf, asdfgh",
        gender_field= "M",
        source="XYZ"))
    snt = Sanitizer()
    print("final output", snt.toRawEntity(test))