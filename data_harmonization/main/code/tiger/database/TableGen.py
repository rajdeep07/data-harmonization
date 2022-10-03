from fileinput import filename
import os
import os.path as path
from pathlib import Path
import pandas as pd
import argparse

class TableGen:
    bottom_code = "from sqlalchemy.orm import declarative_base\n\nBase = declarative_base()"

    import_statement = """from sqlalchemy import BigInteger, Text, Float, Column
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker, relationship
from data_harmonization.main.code.tiger.database.Tables.Bottom import Base\n\n
"""
    attrtype_list = {
        'int64':'Column(BigInteger)\n',
        'float64':'Column(Float)\n',
        'object64':'Column(Text)\n'
    }
    repr_code = "\n\tdef __repr__(self) -> str:\n\t\treturn "

    default_type = 'Column(Text)\n'

    def __init__(self, file_path:path, output_path:path) -> None:
        self.code = ""
        self.file_path = file_path
        self.output_path = output_path
    
    def _update_init(self, filename):
        with open(f'{self.output_path}/__init__.py', 'a') as file:
            file.write(f"from Tables.{filename} import {filename}\n")

    def _bottom_gen(self):
        if path.exists(os.path.join(self.output_path, 'Bottom.py')):
            return
        with open(f'{self.output_path}/Bottom.py', 'w') as file:
            file.write(self.bottom_code)

    def _import_statement_gen(self):
        self.code += self.import_statement

    def _class_gen(self, table_name : str, attr_dict : dict):
        class_code = f"\nclass {table_name.capitalize()}(Base):\n".lstrip()
        class_code += f"\t__tablename__ = '{table_name}'\n\n\tid=Column(BigInteger, primary_key=True)\n"
        for column_name, col_dtype in attr_dict.items():
            # print(str(col_dtype), col_dtype.name, col_dtype)
            if column_name == "Unnamed: 0":
                continue
            col_type = self.attrtype_list.get(str(col_dtype), self.default_type)
            class_code += f"\t{column_name} = {col_type}"
        # self.code = self.code.rstrip()
        self.code += class_code

    def _repr_gen(self, table_name, attr_dict):
        self.repr_code += f"f'<{table_name.capitalize()} "
        for key in attr_dict.keys():
            if key == "Unnamed: 0":
                continue
            self.repr_code += f"{key}:{{self.{key}}} "
        self.repr_code += ">'"
        self.code += self.repr_code

    def _make_file(self, filename):
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        with open(f'{self.output_path}/{filename}.py', 'w') as file:
            file.write(self.code)

    def generate_class(self) -> None:
        df = pd.read_csv(self.file_path, nrows=25)
        table_name = Path(self.file_path).stem
        attr_dict = dict(df.dtypes)


        self._import_statement_gen()
        self._class_gen(table_name ,attr_dict)
        self._repr_gen(table_name, attr_dict)
        self._make_file(table_name.capitalize())
        self._update_init(table_name.capitalize())
        self._bottom_gen()
        # print(self.code)
            

        



if __name__ == "__main__":
    parser = argparse.PARSER
    tgen = TableGen('/home/navazdeens/data-harmonization/data_harmonization/main/data/pbna.csv', 
            '/home/navazdeens/data-harmonization/data_harmonization/main/code/tiger/database/Tables')
    tgen.generate_class()
