from data_harmonization.main.code.com.tiger.data.transformer.utils import StringSupport
from data_harmonization.main.code.com.tiger.data.transformer import CityTransformer, NameTransformer, \
    PostalAddressTransformer, StateTransformer, ZipTransformer, IntegerTypeTransformer, StringTypeTransformer
from data_harmonization.main.code.com.tiger.data.model import GeocodedAddress, PostalAddress
from data_harmonization.main.code.com.tiger.data.model.dataclass import RawEntity, CleansedRawEntity
from data_harmonization.main.code.com.tiger.data.model.datamodel import *


class Sanitizer():

    def _get_attr_list(self, should_print=False):
        items = RawEntity.__dict__.items()
        if should_print:
            [print(f"attribute: {k}  value: {v}") for k, v in items]
        return items

    def toRawEntity(self, cls):

        raw_attribute_lists = self._get_attr_list(RawEntity)
        cls_map = {key:globals[value.replace("Optional[", "").replace("]","")] for key , value in raw_attribute_lists} #{id:"int", "name":"Name"}

        raw_kw = {}
        for raw_attribute, type in raw_attribute_lists.items():
            kw = {}
            if not isinstance(int, cls_map[raw_attribute]) and not isinstance(str, cls_map[raw_attribute]):
                for attr in self._get_attributes(cls_map[raw_attribute]):
                    if hasattr(cls, attr):
                        kw[attr] = cls.__getattribute__(attr)
                raw_kw[raw_attribute] = cls_map[raw_attribute](**kw)
            else:
                if hasattr(cls, raw_attribute):
                    raw_kw[raw_attribute] = cls.__getattribute__(raw_attribute)

        raw_entity_object = RawEntity(**raw_kw)
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

    @classmethod
    def __getattribute__(cls, attr):
        try:
            return {k: _apply_transformer(v) in cls.__dict__[attr].items()}
        except AttributeError:
            raise AttributeError(attr)

    def _get_sanitized_entity(self):
        for attr, val in __getattribute__().items():
            setattr(self, CleansedRawEntity, attr, val)
