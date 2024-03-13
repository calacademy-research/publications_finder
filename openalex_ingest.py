""" 
Take results from API calls written in openalex.py and ingest them into
the corresponding MySQL table. 
"""
from db_connection import DBConnection
# from config import Config
from openalex import OpenAlex

class OpenAlexIngest:
    """Ingests data from API calls that have been restructured for sql insertion."""
    # we will keep this regardless
    def __init__(self, query_option):
        """
        Initialize the class.

        Parameters:
        query_option (str): What to query by. The options are "by_affiliation" or "by_author", 
        which will query the API either by affiliation or by author. 
        """
        # self.table is set by query_data().
        # This is the sql table that records will be inserted into.
        self.table = None

        if query_option not in ["by_affiliation", "by_author"]:
            raise ValueError("query_option must be 'by_affiliation' or 'by_author'")
        else:
            self.data = self.query_data(query_option)

        # Joe - create the table here.
        sql_create_table = f""" CREATE TABLE IF NOT EXISTS {self.table} (
                                            work_id VARCHAR(255) NOT NULL,
                                            work_doi TINYTEXT,
                                            work_title VARCHAR(1000),
                                            work_display_name VARCHAR(1000),
                                            work_publication_year INT,
                                            work_publication_date DATE,
                                            author_id VARCHAR(40) NOT NULL,
                                            author_orcid VARCHAR(40),
                                            author_name TINYTEXT,
                                            author_raw_name TINYTEXT,
                                            author_position VARCHAR(10),
                                            institution_id VARCHAR(255) NOT NULL,
                                            institution_name TINYTEXT,
                                            institution_country_code TINYTEXT, 
                                            PRIMARY KEY (work_id, author_id, institution_id)
                                            );
                                            """
        DBConnection.execute_query(sql_create_table)

    def query_data(self, query_option):
        """assigns self.data based on the option passed to OpenAlexIngest"""
        # I think I can collapse into one self.data = just one table instead of 2 different ones
        # and then separate records based on CAS only with a sql query.
        # the fields for author ended up being the same.
        open_alex = OpenAlex()
        if query_option == 'by_affiliation':
            self.table = "collab_pubs"
            works = open_alex.query_by_affiliation()
            return works
        elif query_option == 'by_author':
            self.table = 'cas_by_author_pubs'
            works = open_alex.query_by_author()
            return works

    def insert_works(self):
        '''Insert works into table. 
    
        Args:
        works: list of dictionaries returned from query_data()
        '''
          # Use insert ignore to skip records where an author's affiliation
            # is repeated for the same publication.
        sql = f"""
                INSERT IGNORE INTO {self.table}
                (work_id, work_doi, work_title, work_display_name, work_publication_year,
                work_publication_date, author_id, author_orcid, author_name, author_raw_name,
                author_position, institution_id, institution_name, institution_country_code)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            for item in self.data:
                DBConnection.execute_query(sql, (
                    item['work_id'],
                    item['work_doi'],
                    item['work_title'],
                    item['work_display_name'],
                    item['work_publication_year'],
                    item['work_publication_date'],
                    item['author_id'],
                    item['author_orcid'],
                    item['author_name'],
                    item['author_raw_name'],
                    item['author_position'],
                    item['institution_id'],
                    item['institution_name'],
                    item['institution_country_code']
                ))
            # conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
