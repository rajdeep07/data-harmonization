from pprint import pprint

# from data_harmonization.main.code.tiger.model.ingester import *
import data_harmonization.main.resources.config as config
import mysql.connector
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from data_harmonization.main.code.tiger.model.ingester.Bottom import Base
from sqlalchemy import text
import data_harmonization.main.resources.config as config_


class MySQL:
    def __init__(self, host, database, user, password):
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.mydb = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        self.url = f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}"
        self.engine = sqlalchemy.create_engine(self.url, echo=False)
        self.mycursor = self.mydb.cursor()
        Base.metadata.create_all(self.engine)

    def get_tables(self):
        return list(map(lambda x: x.capitalize(), Base.metadata.tables.keys()))

    def get_col_counts(self):
        stmt = text(
            """SELECT col,cnt from
        (SELECT c.COLUMN_NAME as col, count(*) as cnt
        FROM information_schema.TABLES as t
        INNER JOIN information_schema.COLUMNS as c   ON t.TABLE_NAME=c.TABLE_NAME
        WHERE t.TABLE_SCHEMA = 'data_harmonization' GROUP BY c.COLUMN_NAME)
        tbl WHERE cnt >= 2;"""
        )
        with self.SessionMaker() as session:
            common_col = self.engine.execute(stmt)
            print(common_col.all())

    def get_result(self, to_print=False, title=""):
        result = self.mycursor.fetchall()
        if to_print:
            print("\n")
            if title:
                print(f"{title}")

            for x in result:
                print(x)

        return result

    def SessionMaker(self) -> sessionmaker:
        session_maker = sessionmaker()
        session_maker.configure(bind=self.engine)
        return session_maker()


if __name__ == "__main__":
    msql = MySQL("localhost", "data_harmonization", "root", config_.mysqlPassword)
    session = msql.SessionMaker()
    print(session)
    # new = Flna(id=3, Name="new name2")
    # session.add(new)
    # session.commit()
    # print(session.query(Pbna).all())
