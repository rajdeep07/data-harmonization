from data_harmonization.main.code.tiger.transformer.utils.StringSupport import *
from data_harmonization.main.code.tiger.model.datamodel import *
from data_harmonization.main.code.tiger.model.datamodel import Address
from data_harmonization.main.code.tiger.transformer.CityTransformer import *
from data_harmonization.main.code.tiger.transformer.StateTransformer import *
from data_harmonization.main.code.tiger.transformer.ZipCodeTransformer import *
from data_harmonization.main.code.tiger.transformer.IntegerTypeTransformer import *
from data_harmonization.main.code.tiger.transformer.StringTypeTransformer import *
from typing import Optional


class PostalAddressTransformer(StringSupport):
    def __init__(self):
        super().__init__(self)

    @staticmethod
    def _get_attr_list(obj, should_print=False):
        items = obj.__dict__.items()
        if should_print:
            [print(f"attribute: {k}  value: {v}") for k, v in items if k in ("__annotations__")]
        return(value for item, value in items if item in ("__annotations__"))

    @staticmethod
    def standardizePostalAddress(value: Address):
        stringSupportObj = StringSupport()
        sub_attr_list = list(PostalAddressTransformer._get_attr_list(Address))[0]
        for sub_attr in sub_attr_list.keys():
            if stringSupportObj.trimAndLowerCase(sub_attr) == "zipcode" or \
                    stringSupportObj.trimAndLowerCase(sub_attr) == "zip":
                if isinstance(getattr(value, sub_attr), str):
                    setattr(value, sub_attr, ZipCodeTransformer.standardizeZipCode(
                        getattr(value, sub_attr)))
                if isinstance(getattr(value, sub_attr), int):
                    setattr(value, sub_attr, int(ZipCodeTransformer.standardizeZipCode(
                        str(getattr(value, sub_attr)))))
            elif stringSupportObj.trimAndLowerCase(sub_attr) == "city":
                setattr(value, sub_attr, CityTransformer.standardizeCity(
                    getattr(value, sub_attr)))
            elif isinstance(getattr(value, sub_attr), str):
                setattr(value, sub_attr, StringTypeTransformer.standardizeStringType(
                    getattr(value, sub_attr)))
            elif isinstance(getattr(value, sub_attr), int):
                setattr(value, sub_attr, IntegerTypeTransformer.standardizeIntegerType(
                    getattr(value, sub_attr)))
        return value



