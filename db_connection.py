import logging
import re
import mysql.connector
import yaml
import traceback
import time
from pymysql.converters import escape_string

class DBConnector(object):

    def __init__(self):
        self.dbconn = None
        self.db_config = self.read_db_config()

    def read_db_config(self):
        with open('./db_connection.yml', 'r') as file:
            config = yaml.safe_load(file)
        return config


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
    def log_sql(cls, query, stack_trace):
        # for debugging, enable if there are issues.
        # works with ./vm/sql_log_scan.sh
        return
        formatted_stack_trace = ''.join(stack_trace)
        log_string = (
            f"SQL Query: {query}\n"
            "Stack Trace:\n"
            f"{formatted_stack_trace}\n"
            f"{'-' * 40}\n"
        )
        with open('./sql.log', 'a') as log_file:
            log_file.write(log_string)
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

        # Regex to match queries that are expected to return a result set.
        returns_result_pattern = re.compile(r"^\s*(WITH|SELECT)", re.IGNORECASE)

        for attempt in range(max_retries):
            connection = cls.get_connection()
            try:
                cursor = connection.cursor()
                if args is None:
                    cursor.execute(query)
                    formatted_query = query
                else:
                    cursor.execute(query, args)
                    formatted_query = query
                    for arg in args:
                        formatted_query = formatted_query.replace('%s', f"'{escape_string(str(arg))}'", 1)

                cls.log_sql(formatted_query, traceback.format_stack())
                # Check if query is expected to return a result. 
                # Had to make this more flexible than just matching SELECT at the beginning of query
                # to also handle CTEs.
                if returns_result_pattern.match(query):
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


