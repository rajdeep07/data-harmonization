# from data_harmonization.main.code.tiger.transformer.utils import StringSupport
# from data_harmonization.main.code.tiger.transformer import CityTransformer, NameTransformer, \
#     PostalAddressTransformer, StateTransformer, ZipTransformer, IntegerTypeTransformer, StringTypeTransformer
# from data_harmonization.main.code.tiger.model import GeocodedAddress, PostalAddress
# from data_harmonization.main.code.tiger.model.datamodel import RawEntity, CleansedRawEntity
from dataclasses import dataclass
from data_harmonization.main.code.tiger.model.datamodel import *
from typing import Any


class Sanitizer():

    def _get_attr_list(self, obj, should_print=False):
        items = obj.__dict__.items()
        if should_print:
            [print(f"attribute: {k}  value: {v}") for k, v in items if k in ("__annotations__")]
        return(value for item, value in items if item in ("__annotations__"))

    def get_kwargs(self, cls, attr_lists):
        cls_map={}
        for attr, dtype in attr_lists.items():
            dtype = dtype.replace("Optional[","").replace("]","")
            cls_map[attr] = globals().get(dtype, dtype)
        

        raw_kw = {}
        for attr, tpe in cls_map.items():
            kw={}
            if tpe not in ('int', 'str', 'float'):
                sub_attr_list = list(self._get_attr_list(cls_map[attr]))[0]
                for sub_attr in sub_attr_list.keys():
                    if hasattr(cls, sub_attr):
                        kw[sub_attr] = getattr(cls, sub_attr)   
                raw_kw[attr] = cls_map[attr](**kw)
            else:
                if hasattr(cls, attr):
                    raw_kw[attr] = getattr(cls, attr)
        return raw_kw

    def toRawEntity(self, cls):

        raw_attribute_lists = list(self._get_attr_list(RawEntity))[0]
        raw_kw = self.get_kwargs(cls, raw_attribute_lists)
        raw_entity_object = RawEntity(**raw_kw)
        print(raw_entity_object)
        return raw_entity_object
    
    def _apply_transformer(self):
        transformed_ = {}
        for value in _get_attr_list():
            if isinstance(value[1], list):
                # TODO: add more general type standardization like Gender, email etc.
                if isinstance(value[1][0], Name):
                    transformed_[value[0]] = standardizeName(value[1][0])
                elif isinstance(value[1][0], Address):
                    transformed_[value[0]] = standardizePostalAddress(value[1][0])
                elif isinstance(value[1], int):
                    transformed_[value[0]] = standardizeIntegerType(value[1][0])
                elif isinstance(value[1], str):
                    transformed_[value[0]] = standardizeStringType(value[1][0])
                elif value[1] is None:
                    transformed_[value[0]] = standardizeStringType(value[1][0])
            elif isinstance(value[1], int):
                transformed_[value[0]] = standardizeIntegerType(value[1][0])
            elif isinstance(value[1], str):
                transformed_[value[0]] = standardizeStringType(value[1][0])
            elif value[1] is None:
                transformed_[value[0]] = standardizeStringType(value[1][0])

        return transformed_

    # @classmethod
    # def __getattribute__(cls, attr):
    #     try:
    #         return {k: cls._apply_transformer(v) for k, v in cls.__dict__[attr].items()}
    #     except AttributeError:
    #         raise AttributeError(attr)

    def _get_sanitized_entity(self):
        for attr, val in self.__getattribute__().items():
            setattr(self, CleansedRawEntity, attr, val)

if __name__ == "__main__":
    @dataclass
    class TestClass:
        name="name"
        id=1
        city = "new_city"
        zipcode = 123456
        address = "123, A st, new_city"
        gender_field= "M"
        source="XYZ"
    snt = Sanitizer()
    snt.toRawEntity(TestClass)

