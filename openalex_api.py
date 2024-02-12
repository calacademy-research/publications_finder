from db_connection import DBConnection
from config import Config
class OpenAlex:
    def __init__(self):
        self.config = Config()
        # Joe - create the table here.
        sql_create_database_table = """ CREATE TABLE IF NOT EXISTS dois (
                                          doi varchar(255) not null  primary key

                                    );"""
        DBConnection.execute_query(sql_create_database_table)
        self.ROR = self.config.get_string("alex_param", "ROR")

