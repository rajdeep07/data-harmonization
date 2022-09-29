import data_harmonization.main.resources.config as config
import mysql.connector
import sqlalchemy


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
        self.engine = sqlalchemy.create_engine(self.url)
        self.mycursor = self.mydb.cursor()

    def get_result(self, to_print=False, title=''):
        result = self.mycursor.fetchall()
        if to_print:
            print('\n')
            if title:
                print(f'{title}')

            for x in result:
                print(x)

        return result

if __name__ == "__main__":
    msql = MySQL('localhost', 'data-harmonization', 'navazdeen.shamsu', 'root@123')