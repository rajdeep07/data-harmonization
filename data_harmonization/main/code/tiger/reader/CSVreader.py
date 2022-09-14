from ast import Add
import string
from os import strerror
import csv
from pydantic import BaseModel
from typing import List
from data_harmonization.main.code.com.tiger.data.model.datamodel import *

class CSVreader(object):

    def __init__(self, file):
        self.file = file

    def get_file(self):
        entities = []
        try:
            with open(self.file, "r") as f:
                self.reader = csv.reader(f)
                headers = next(self.reader)
                for row in self.reader:
                    row_data = {key: value for key, value in zip(headers, row)}
                    _name = Name(name=row_data['name'])
                    _address = Address(city=row_data['city'], state=row_data['state'], zipcode=row_data['zipcode'], address=row_data['address'])
                    _raw_entity = RawEntity(id=row_data['id'], name=List[_name], address=List[_address],source=row_data['source'])
                    entities.append(_raw_entity)
            return entities
        except IOError as err:
            print("I/O error({0}): {1}".format(err, strerror))
        return entities

    def get_num_rows(self):
        try:
            with open(self.file, "r") as f:
                self.reader = [row for row in csv.reader(f, delimiter=",")]
        except IOError as err:
            print("I/O error({0}): {1}".format(err, strerror))

        print(sum(1 for row in self.reader))
        return