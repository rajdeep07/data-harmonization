from curses import echo
from data_harmonization.main.code.tiger.database.Tables.Pbna import Pbna
import data_harmonization.main.resources.config as config
import mysql.connector
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from data_harmonization.main.code.tiger.database.Tables.Bottom import Base


class MySQL:
    def __init__(self, host, database, user, password):
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.mydb = mysql.connector.connect(
            host = self.host,
            user = self.user,
            password = self.password,
            database = self.database
        )
        self.url = f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}'
        self.engine = sqlalchemy.create_engine(self.url, echo=True)
        self.mycursor = self.mydb.cursor()
        Base.metadata.create_all(self.engine)

    def get_result(self, to_print=False, title=''):
        result = self.mycursor.fetchall()
        if to_print:
            print('\n')
            if title:
                print(f'{title}')

            for x in result:
                print(x)

        return result
    
    def SessionMaker(self) -> sessionmaker:
        session_maker = sessionmaker()
        session_maker.configure(bind=self.engine)
        return session_maker()

if __name__ == "__main__":
    msql = MySQL('localhost', 'data_harmonization', 'root', 'root$Navaz1')
    session = msql.SessionMaker()
    new = Pbna(id=1, Name="new name")
    for res in session.query(Pbna).all():
        print(res)
    