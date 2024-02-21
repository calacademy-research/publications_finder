from db_connection import DBConnection
from config import Config
class OpenAlex:
    # we will keep this regardless
    def __init__(self):
        self.config = Config()
        # Joe - create the table here.
        sql_create_database_table = """ CREATE TABLE IF NOT EXISTS works (
                                            work_id TINYTEXT NOT NULL PRIMARY KEY,
                                            work_doi TINYTEXT,
                                            work_title VARCHAR(1000),
                                            work_display_name VARCHAR(1000),
                                            work_publication_year INT,
                                            work_publication_date DATE,
                                            author_id VARCHAR(40),
                                            author_orcid VARCHAR(40),
                                            author_name TINYTEXT,
                                            author_raw_name TINYTEXT),
                                            author_position VARCHAR(10)
                                    );"""
        DBConnection.execute_query(sql_create_database_table)
        self.ROR = self.config.get_string("alex_param", "ROR")

    def populate_instutition(self):
        # this is somehing maybe we call out to a third party API? maybe we use your
        # existing code to query and do the pagination and all that good stuff
        pass

    def query_by_author(self):
        pass

    def insert_doi(self,array):
        sql = f"insert bla bla into yadda {variable}"
        DBConnection.execute_query(sql)


