import os
import os.path as path
from pathlib import Path
import pandas as pd
import argparse

class TableGen:
    import_statement = """from sqlalchemy import Integer, Column, String, Float, ForeignKey
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker, relationship
from data_harmonization.main.code.tiger.database.Tables.Bottom import Base\n\n
"""
    attrtype_list = {
        'int64':'Column(Integer)\n',
        'float64':'Column(Float)\n',
        'object64':'Column(String)\n'
    }
    default_type = 'Column(String)\n'

    def __init__(self, file_path:path, output_path:path) -> None:
        self.code = ""
        self.file_path = file_path
        self.output_path = output_path

    def _import_statement_gen(self):
        self.code += self.import_statement 

    def _class_gen(self, table_name : str, attr_dict : dict):
        class_code = f"\nclass {table_name.capitalize()}(Base):\n".lstrip()
        class_code += f"\t__tablename__ = '{table_name}'\n\n\tid=Column(Integer(), primary_key=True)\n"
        for column_name, col_dtype in attr_dict.items():
            # print(str(col_dtype), col_dtype.name, col_dtype)
            if column_name == "Unnamed: 0":
                continue
            col_type = self.attrtype_list.get(str(col_dtype), self.default_type)
            class_code += f"\t{column_name} = {col_type}"
        # self.code = self.code.rstrip()
        self.code += class_code

    def _make_file(self, filename):
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        with open(f'{self.output_path}/{filename}.py', 'w') as file:
            file.write(self.code)

    def generate_class(self) -> None:
        df = pd.read_csv(self.file_path, nrows=25)
        table_name = Path(self.file_path).stem
        attr_dict = dict(df.dtypes)
        print(attr_dict['Zip'] == 'int')


        self._import_statement_gen()
        self._class_gen(table_name ,attr_dict)
        self._make_file(table_name.capitalize())
        # print(self.code)
            

        



if __name__ == "__main__":
    parser = argparse.PARSER
    tgen = TableGen('/home/navazdeens/data-harmonization/data_harmonization/main/data/pbna.csv', 
            '/home/navazdeens/data-harmonization/data_harmonization/main/code/tiger/database/Tables')
    tgen.generate_class()
