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
        query_option (str): What to query by. The options are "by_affiliation" or "by_author_orcid", 
        which will query the API either by affiliation or by author orcid. 
        """
        # self.table is set by query_data().
        # This is the sql table that records will be inserted into.
        # self.table = None

        if query_option not in ["by_affiliation", "by_author_orcid"]:
            raise ValueError("query_option must be 'by_affiliation' or 'by_author_orcid'")
        else:
            self.data = self.query_data(query_option)

        # Joe - create the table here.
        sql_create_table = """ CREATE TABLE IF NOT EXISTS comprehensive_global_works_v2 (
                                            work_id VARCHAR(255) NOT NULL,
                                            work_doi TINYTEXT,
                                            work_title VARCHAR(1000),
                                            work_display_name VARCHAR(1000),
                                            work_publisher VARCHAR(1000),
                                            work_journal VARCHAR(1000),
                                            work_publication_year INT,
                                            work_publication_date DATE,
                                            work_sustainable_dev_goal VARCHAR(1000),
                                            author_id VARCHAR(40) NOT NULL,
                                            author_orcid VARCHAR(40),
                                            author_name TINYTEXT,
                                            author_raw_name TINYTEXT,
                                            institution_id VARCHAR(255) NOT NULL,
                                            institution_name TINYTEXT,
                                            institution_country_code TINYTEXT, 
                                            PRIMARY KEY (work_id, author_id, institution_id)
                                            );
                                            """
        DBConnection.execute_query(sql_create_table)

    def query_data(self, query_option):
        """Returns works object based on the option passed to OpenAlexIngest"""
        open_alex = OpenAlex()
        if query_option == 'by_affiliation':
            works = open_alex.query_by_affiliation()
            return works
        works = open_alex.query_by_author()
        return works

    def insert_works(self):
        '''Insert works into table by iterating over self.data.'''
          # Use insert ignore to skip records where an author's affiliation
            # is repeated for the same publication. This info is redundant. 
        sql = """
                INSERT IGNORE INTO comprehensive_global_works_v2
                (work_id, work_doi,
                work_title, work_display_name, work_publisher, work_journal,
                work_publication_year, work_publication_date, work_sustainable_dev_goal,
                author_id, author_orcid, author_name, author_raw_name,
                institution_id, institution_name, institution_country_code)
                VALUES
                (%s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s)
        """
        # 15 fields
        try:
            for item in self.data:
                DBConnection.execute_query(sql, (
                    item['work_id'],
                    item['work_doi'],
                    item['work_title'],
                    item['work_display_name'],
                    item['work_publisher'],
                    item['work_journal'],
                    item['work_publication_year'],
                    item['work_publication_date'],
                    item['work_sustainable_dev_goal'],
                    item['author_id'],
                    item['author_orcid'],
                    item['author_name'],
                    item['author_raw_name'],
                    # item['author_position'],
                    item['institution_id'],
                    item['institution_name'],
                    item['institution_country_code']
                ))
            # conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")


    def remove_works(self):
        """
        Remove works from authors whose affiliation data is incorrect on OpenAlex. 

        """
        sql_rm_authors = """
                DELETE FROM comprehensive_global_works_v2
                WHERE author_id IN (
                'https://openalex.org/A5048870777', 
                'https://openalex.org/A5086404490',
                'https://openalex.org/A5053358298',
                'https://openalex.org/A5046354008',
                'https://openalex.org/A5019535042'
                )
        """
        try:
            DBConnection.execute_query(sql_rm_authors)
        except Exception as e:
            print(f"Failed to execute query: {e}")