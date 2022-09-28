import uuid
from dataclasses import dataclass
from typing import Any

import pandas as pd

# from data_harmonization.main.code.tiger.model.dataclass import RawEntity, CleansedRawEntity
from data_harmonization.main.code.tiger.model.datamodel import *
from data_harmonization.main.code.tiger.transformer.IntegerTypeTransformer import \
    IntegerTypeTransformer
from data_harmonization.main.code.tiger.transformer.NameTransformer import \
    NameTransformer
from data_harmonization.main.code.tiger.transformer.PostalAddressTransformer import \
    PostalAddressTransformer
from data_harmonization.main.code.tiger.transformer.StringTypeTransformer import \
    StringTypeTransformer


class Sanitizer:
    def _get_attr_list(self, obj, should_print=False):
        items = obj.__dict__.items()
        if should_print:
            [
                print(f"attribute: {k}  value: {v}")
                for k, v in items
                if k in ("__annotations__")
            ]
        return (value for item, value in items if item in ("__annotations__"))

    def get_kwargs(self, cls, attr_lists, gen_id=False, clean_data=True):
        self.cls_map = {}
        for attr, dtype in attr_lists.items():
            dtype = dtype.replace("Optional[", "").replace("]", "")
            self.cls_map[attr] = globals().get(dtype, dtype)
        raw_kw = {}
        for attr, tpe in self.cls_map.items():
            kw = {}
            if attr == "id" and gen_id:
                raw_kw[attr] = uuid.uuid4()
            elif tpe not in ("int", "str", "float"):
                sub_attr_list = list(self._get_attr_list(self.cls_map[attr]))[0]
                for sub_attr in sub_attr_list.keys():
                    try:
                        kw[sub_attr] = cls.get(sub_attr, None)
                    except:
                        kw[sub_attr] = getattr(cls, sub_attr, None)
                
                raw_kw[attr] = self._apply_transformer(kw, tpe) if clean_data else kw
            else:
                try:
                    raw_kw[attr] = self._apply_transformer(cls.get(attr, None))  if clean_data else cls.get(attr, None)  # for dict conversion
                except:
                    raw_kw[attr] = self._apply_transformer(getattr(cls, attr, None)) if clean_data else getattr(cls, attr, None)  # for class conversion

        return raw_kw

    def toRawEntity(self, cls:Entity1, clean_data:bool) -> RawEntity:
        raw_attribute_lists = list(self._get_attr_list(RawEntity))[0]
        raw_kw = self.get_kwargs(cls, attr_lists=raw_attribute_lists, clean_data=clean_data)
        raw_entity_object = RawEntity(**raw_kw)
        return raw_entity_object
    
    def toEntity(self, Ent_Obj : Any, data:dict, gen_id=True):
        entity_attr_list = list(self._get_attr_list(Ent_Obj))[0]
        kw = self.get_kwargs(data, entity_attr_list, gen_id)
        entity_obj = Ent_Obj(**kw)
        return entity_obj

    def _apply_transformer(self, attr, cls=None):
        transformed_ = {}
        if isinstance(attr, int):
            transformed_ = IntegerTypeTransformer.standardizeIntegerType(str(attr))
        elif isinstance(attr, str):
            transformed_ = StringTypeTransformer.standardizeStringType(attr)
        elif isinstance(attr, list):
            transformed_list = []
            for item in attr:
                if isinstance(item, int):
                    transformed_list.append(
                        IntegerTypeTransformer.standardizeIntegerType(item)
                    )
                elif isinstance(item, str):
                    transformed_list.append(
                        StringTypeTransformer.standardizeStringType(item)
                    )
            transformed_ = transformed_list
        elif attr is None:
            transformed_ = attr
        else:
            attr_value = cls(**attr)
            if isinstance(attr_value, Name):
                transformed_ = NameTransformer.standardizeName(attr_value)
            elif isinstance(attr_value, Address):
                transformed_ = PostalAddressTransformer.standardizePostalAddress(
                    attr_value
                )
            # elif isinstance(attr_value, Gender):
            #     transformed_ = standardizeGender(attr_value)
            # elif isinstance(attr_value, Email):
            #     transformed_ = standardizeEmail(attr_value)
            # elif isinstance(attr_value, Contact):
            #     transformed_ =  standardizeContact(attr_value)
            elif isinstance(attr_value, int):
                transformed_ = IntegerTypeTransformer.standardizeIntegerType(attr_value)
            elif isinstance(attr_value, str):
                transformed_ = StringTypeTransformer.standardizeStringType(attr_value)
            elif attr_value is None:
                transformed_ = attr_value
            elif isinstance(attr_value, list):
                transformed_list = []
                for item in attr_value:
                    if isinstance(item, int):
                        transformed_list.append(
                            IntegerTypeTransformer.standardizeIntegerType(item)
                        )
                    elif isinstance(item, str):
                        transformed_list.append(
                            StringTypeTransformer.standardizeStringType(item)
                        )
                transformed_ = transformed_list
            else:
                kw = {}
                items = attr_value.__dict__.items()
                for item, value in items:
                    if value:
                        if isinstance(value, int):
                            kw[item] = IntegerTypeTransformer.standardizeIntegerType(
                                str(value)
                            )
                        if isinstance(value, str):
                            kw[item] = StringTypeTransformer.standardizeStringType(
                                value
                            )
                        else:
                            kw[item] = self._apply_transformer(item, attr_value)
                    else:
                        kw[item] = value
                transformed_ = cls(**kw)
        return transformed_

    """@classmethod
    def __getattribute__(cls, attr):
        try:
            return {k: _apply_transformer(v) in cls.__dict__[attr].items()}
        except AttributeError:
            raise AttributeError(attr)"""

    """def _get_sanitized_entity(self):
        for attr, val in __getattribute__().items():
            setattr(self, CleansedRawEntity, attr, val)"""


if __name__ == "__main__":
    addr = Address(city="Kolkata!22##*!?@34", zipcode=700000, address="Saltlake + Sdfg")
    entity1 = Entity1(
        id=12,
        name="ABC",
        city="Kalyani",
        state="WB",
        zipcode=741250,
        address="Bedibhawan",
        source="src",
        gender="F",
    )
    test = pd.Series(
        dict(
            name="tiger Analytics",
            city="new_city",
            zipcode=123456,
            address="231, asdf, asdfgh",
            gender="M",
            source="XYZ",
            age=25,
        )
    )
    snt = Sanitizer()
    print("Entity:\n", snt.toEntity(Entity1, test))
    print("RawEntity:\n", snt.toRawEntity(entity1, clean_data=False))
