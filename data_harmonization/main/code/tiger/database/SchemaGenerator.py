from fileinput import filename
import os
import os.path as path
from os import listdir
from pathlib import Path
import pandas as pd


class SchemaGenerator:
    bottom_code = (
        "from sqlalchemy.orm import declarative_base\n\nBase = declarative_base()"
    )

    import_statement = """from sqlalchemy import BigInteger, Text, Float, Column
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from data_harmonization.main.code.tiger.model.ingester.Bottom import Base\n\n
"""
    attrtype_list = {
        "int64": "Column(BigInteger)\n",
        "float64": "Column(Float)\n",
        "object64": "Column(Text)\n",
    }
    repr_code = "\n\tdef __repr__(self) -> str:\n\t\treturn "

    schema_code = "\n\t@staticmethod\n\tdef get_schema() -> dict:\n"

    default_type = "Column(Text)\n"

    def __init__(self, file_path: path = None, output_path: path = None) -> None:
        self.code = ""
        self.file_path = file_path
        self.output_path = output_path

    def _update_init(self, filename, alias: str = ""):
        lines = set()
        try:
            with open(f"{self.output_path}/__init__.py", "r") as file:
                lines = set(file.readlines())
                # print(lines)
                lines.add(f"from data_harmonization.main.code.tiger.model.ingester.{filename} import {filename}\n")
        except FileNotFoundError:
            print(
                f"{str(FileNotFoundError.errno)} initializing ingester module")
            lines.add(f"from data_harmonization.main.code.tiger.model.ingester.{filename} import {filename}\n")

        with open(f"{self.output_path}/__init__.py", "w") as file:
            for line in lines:
                file.write(line)

    def _bottom_gen(self):
        if path.exists(os.path.join(self.output_path, "Bottom.py")):
            return
        with open(f"{self.output_path}/Bottom.py", "w") as file:
            file.write(self.bottom_code)

    def _import_statement_gen(self):
        self.code += self.import_statement

    def _class_gen(self, table_name: str, attr_dict: dict):
        class_code = f"\nclass {table_name.capitalize()}(Base):\n".lstrip()
        class_code += f"\t__tablename__ = '{table_name}'\n\n\tid=Column(BigInteger, primary_key=True)\n"
        for column_name, col_dtype in attr_dict.items():
            # print(str(col_dtype), col_dtype.name, col_dtype)
            if column_name == "Unnamed: 0" or column_name.lower() == "id":
                continue
            col_type = self.attrtype_list.get(
                str(col_dtype), self.default_type)
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
        with open(f"{self.output_path}/{filename}.py", "w") as file:
            file.write(self.code)

    def _schema_method(self, table_name, dict_types):
        dict_types["id"] = "int64"
        dict_types = {k: str(v)
                      for k, v in dict_types.items() if k != "Unnamed: 0"}
        self.schema_code += f"\t\treturn {dict_types}\n"
        self.code += self.schema_code

    def generate_class(self, alias: str = "") -> None:
        # read sample 25 rows of CSV to infer schema
        df = pd.read_csv(self.file_path, nrows=25)
        table_name = Path(self.file_path).stem
        attr_dict = dict(df.dtypes)

        self._import_statement_gen()
        self._class_gen(table_name, attr_dict)
        self._schema_method(table_name, attr_dict)
        self._repr_gen(table_name, attr_dict)
        self._make_file(table_name.capitalize())
        self._update_init(table_name.capitalize(), alias)
        self._bottom_gen()

    def generate_class_from_schema(
        self, schema: dict, class_name: str, output_path: path or str, alias: str = ""
    ):
        self.output_path = output_path

        self._import_statement_gen()
        self._class_gen(table_name=class_name, attr_dict=schema)
        self._schema_method(table_name=class_name, dict_types=schema)
        self._repr_gen(table_name=class_name, attr_dict=schema)
        self._make_file(class_name.capitalize())
        self._update_init(class_name.capitalize(), alias)


if __name__ == "__main__":

    current_dir = os.path.dirname(os.path.realpath(__file__))
    target_dir = os.path.sep.join(current_dir.split(os.path.sep)[:-3])

    filenames = listdir(target_dir + "/data/")
    csv_filenames = [
        filename
        for filename in filenames
        if filename.endswith(".csv") and not filename.startswith("benchmark")
    ]

    schema_dir = str(target_dir + "/code/tiger/model/ingester/")

    for csv_file in csv_filenames:
        schemaGen = SchemaGenerator(
            str(target_dir + "/data/" + csv_file), schema_dir)
        schemaGen.generate_class()
