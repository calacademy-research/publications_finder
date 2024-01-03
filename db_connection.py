import logging
import mysql.connector
import yaml
import traceback
import time


class DBConnector(object):

    def __init__(self):
        self.dbconn = None
        self.db_config = self.read_db_config()

    def read_db_config(self):
        with open('./vm/vm_passwords.yml', 'r') as file:
            config = yaml.safe_load(file)
        return config

    # def create_connection(self):
    #     return sqlite3.connect('doi_database.db', timeout=30.0)

    def create_connection(self):
        return mysql.connector.connect(
            host=self.db_config['database_url'],
            user=self.db_config['database_user'],  # Replace with your username
            password=self.db_config['database_password'],

            database=self.db_config['database_name'],  # Replace with your database name
            port=self.db_config['database_port']
        )


    # For explicitly opening database connection
    def __enter__(self):
        self.dbconn = self.create_connection()
        return self.dbconn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dbconn.close()


class DBConnection(object):
    connection = None

    @classmethod
    def get_connection(cls, new=False):
        """Creates return new Singleton database connection"""
        if new or not cls.connection:
            cls.connection = DBConnector().create_connection()
        return cls.connection

    @classmethod
    def execute_query(cls, query, args=None):
        """
        Execute a SQL query with retry mechanism on deadlock.

        :param query: The SQL query
        :param args: Arguments for the query
        :return: Query result for SELECT, or None for other types
        """
        max_retries = 3
        retry_delay = 60  # seconds

        for attempt in range(max_retries):
            connection = cls.get_connection()
            try:
                cursor = connection.cursor()
                if args is None:
                    cursor.execute(query)
                else:
                    cursor.execute(query, args)

                if query.strip().upper().startswith("SELECT"):
                    result = cursor.fetchall()
                    cursor.close()
                    return result
                else:
                    connection.commit()
                    cursor.close()
                    return None
            except mysql.connector.errors.InternalError as e:
                if e.errno == 1213:  # Deadlock error code
                    logging.warning(
                        f"Deadlock detected, attempt {attempt + 1} of {max_retries}. Retrying in {retry_delay} seconds.")
                    time.sleep(retry_delay)
                else:
                    raise e
            except Exception as e:
                logging.critical(f"Bad SQL: {e}:\n{query}")
                print(traceback.format_exc())

                cursor.close()
                raise e

        # If all retries fail, rethrow the last exception
        raise Exception(f"Failed to execute query after {max_retries} attempts due to deadlock.")


