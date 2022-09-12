from data_harmonization.main.code.tiger.transformer.utils import StringSupport
from data_harmonization.main.code.tiger.transformer import CityTransformer, NameTransformer, \
    PostalAddressTransformer, StateTransformer, ZipTransformer, IntegerTypeTransformer, StringTypeTransformer
from data_harmonization.main.code.tiger.model import GeocodedAddress, PostalAddress
#from data_harmonization.main.code.tiger.model.dataclass import RawEntity, CleansedRawEntity
from data_harmonization.main.code.tiger.model.datamodel import Name, Address, \
    Entity1, Entity2, RawEntity, Gender, Contact, Email


class Sanitizer():

    def _get_attr_list(self, should_print=False):
        items = RawEntity.__dict__.items()
        if should_print:
            [print(f"attribute: {k}  value: {v}") for k, v in items]
        return items

    def _get_attributes(self, cls, should_print=False):
        items = cls.__dict__.items()
        if should_print:
            [print(f"attribute: {k}  value: {v}") for k, v in items]
        return items

    def _apply_transformer(self, attr):
        transformed_ = {}
        if not isinstance(self._get_attributes(attr), int) and not isinstance(self._get_attributes(attr), str):
            transformed_attr = {}
            cls_map = {}
            for inner_attr in self._get_attributes(attr):
                if isinstance(inner_attr[1], Name):
                    transformed_attr[inner_attr[0]] = standardizeName(inner_attr[1])
                elif isinstance(inner_attr[1], Address) or isinstance(inner_attr[1], PostalAddress):
                    transformed_attr[inner_attr[0]] = standardizePostalAddress(inner_attr[1])
                elif isinstance(inner_attr[1], Gender):
                    transformed_attr[inner_attr[0]] = standardizeGender(inner_attr[1])
                elif isinstance(inner_attr[1], Email):
                    transformed_attr[inner_attr[0]] = standardizeEmail(inner_attr[1])
                elif isinstance(inner_attr[1], Contact):
                    transformed_attr[inner_attr[0]] = standardizeContact(inner_attr[1])
                elif isinstance(inner_attr[1], int):
                    transformed_attr[inner_attr[0]] = standardizeIntegerType(inner_attr[1])
                elif isinstance(inner_attr[1], str):
                    transformed_attr[inner_attr[0]] = standardizeStringType(inner_attr[1])
                elif inner_attr[1] is None:
                    transformed_attr[inner_attr[0]] = standardizeStringType(inner_attr[1])
                elif isinstance(inner_attr, list):
                    transformed_attr[inner_attr[0]] = inner_attr
                else:
                    transformed_[inner_attr[0]] = self._apply_transformer(inner_attr)
                cls_map[inner_attr[0]] = type(inner_attr[1])
            transformed_[attr[0]] = cls_map[attr[0]](**transformed_attr)
        elif isinstance(self._get_attributes(attr), int):
            transformed_[attr[0]] = standardizeIntegerType(_get_attributes(attr)[1])
        elif isinstance(self._get_attributes(attr), str):
            transformed_[attr[0]] = standardizeStringType(_get_attributes(attr)[1])
        elif attr[1] is None:
            transformed_[attr[0]] = attr[1]

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
