"""
Module for querying the publications.comprehensive_global table and returning
works for a certain period. The works are organized by either concatenated authors
or by individual authors. The individual authors option provides role and department information
for each author. The concatenated authors option does not. 
"""
import os
import pandas as pd
import sqlalchemy as sa
import yaml
from db_connection import DBConnection
from config import Config

# Set config variables
config = Config()
FROM_YEAR = config.get_int("years", "from_year")
TO_YEAR = config.get_int("years", "to_year")
RESULTS_BY_INDIVIDUAL_AUTHOR = config.get_boolean("query_results", "single_authors")
CURATORS = config.get_boolean("query_results", "curators")

# Assembling the OUTFILE path
base_path = 'generated_csvs/'
options = ['curators' if CURATORS is True else '',
           'works_by_author' if RESULTS_BY_INDIVIDUAL_AUTHOR is True else 'total_works',
           FROM_YEAR,
           TO_YEAR
        ]
filters = f'{"_".join(map(str,options))}'
OUTFILE = base_path + filters
print ('csv Location: ', OUTFILE)

def check_outfile_directory(dir_path='./generated_csvs/'):
    """Create directory for generated csvs if it doesn't exist."""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

def create_engine():
    """Create the sqlalchemy engine to generate dataframes and csvs
    from query results.
    """
    with open('db_connection.yml', 'r') as file:
        config = yaml.safe_load(file)

    database_url = config['database_url']
    database_port = config['database_port']
    database_password = config['database_password']
    database_user = config['database_user']
    database_name = config['database_name']

    connection_string = f"mysql+pymysql://{database_user}:{database_password}@{database_url}:{database_port}/{database_name}"

    # Return SQL engine
    return sa.create_engine(connection_string)

# Creating the authors table
# sql_create_table = """ CREATE TABLE IF NOT EXISTS authors (
#                                         author_idx INT NOT NULL AUTO_INCREMENT,
#                                         author_wikidata VARCHAR(45),
#                                         author_orcid VARCHAR(45),
#                                         author_alexid VARCHAR(40) NOT NULL,
#                                         author_name TINYTEXT,
#                                         author_raw_name TINYTEXT,
#                                         author_role TINYTEXT,
#                                         author_department VARCHAR(255),
#                                         author_active INT,
#                                         author_notes TINYTEXT,
#                                         PRIMARY KEY (author_idX)
#                                         );
#                                         """
# DBConnection.execute_query(sql_create_table)

# Load data from csv of authors
# Note on how to read from files in Docker SQL: 
# run `SHOW VARIABLES LIKE 'secure_file_priv';` to get the path where files are allowed to be read from
# will probably be: /var/lib/mysql-files/
# then run docker cp /path/to/local/file.csv my_mysql_container:/var/lib/mysql-files/
# change path to your local file and the name/id of the docker container holding your sql db,
# and path if it's different than above

# only need to do this once so commenting out now. 
# future updates will prob just need insert ignore/update
# sql_load_data = """ LOAD DATA INFILE '/var/lib/mysql-files/cas_authorids_v7.tsv'
#             INTO TABLE authors
#             FIELDS TERMINATED BY '\t'
#             LINES TERMINATED BY '\n'
#             IGNORE 1 LINES
#             (author_wikidata,
#             author_orcid,
#             author_alexid,
#             author_name,
#             author_raw_name,
#             author_role,
#             author_department,
#             author_active,
#             author_notes);
# """
# DBConnection.execute_query(sql_load_data)

def concat_authors_works_to_df_csv(engine, output_file=OUTFILE):
    """
    Generate a csv of CAS works with all authors concatenated into one field.
    This is useful for counting all works for a time interval since
    there will not be duplicate works.

    Args:
    engine (sqlalchemy engine instance)
    output_file (str): csv path of query results. Default is OUTFILE

    Returns:
    df: (pandas df) df of query results
    Also saves df as csv into OUTFILE path. 
    """
    query = f"""
        WITH cas_pubs AS (
            SELECT * FROM `publications`.`comprehensive_global_works_v3` 
             WHERE institution_name = 'California Academy of Sciences'
             OR author_orcid in (SELECT author_orcid FROM authors where author_orcid != 'NULL' and author_active=1)
            )
            SELECT 
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                GROUP_CONCAT(DISTINCT author_name SEPARATOR ', ') AS authors_concatenated,
                work_sustainable_dev_goal,
                work_is_open_access,
                work_cited_by_count

            FROM 
                cas_pubs

            GROUP BY 
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                work_sustainable_dev_goal,
                work_is_open_access,
                work_cited_by_count

             HAVING
                 work_publication_year >= {FROM_YEAR}
                 AND work_publication_year <= {TO_YEAR}
            ORDER BY authors_concatenated;
        """
    df = pd.read_sql_query(query, engine)
    df.to_csv(output_file, index=False)
    return df

def single_authors_to_df_csv(engine, output_file, curators=CURATORS):
    """
    Generate a csv of CAS works by individual author.
    This is useful for filtering by individual role, department, etc.
    Not useful for total tally of works because there will be duplicate works.

    Args:
    engine (sqlalchemy engine instance)
    output_file (str): csv path of query results. Default is OUTFILE.

    Returns:
    df: (pandas df) dataframe of query results.
    Also saves csv of dataframe to OUTFILE path.
    """
    query = f"""
        WITH cas_pubs AS (
            SELECT * FROM `publications`.`comprehensive_global_works_v3` 
             WHERE institution_name = 'California Academy of Sciences'
             OR author_orcid in (SELECT author_orcid FROM authors where author_orcid != 'NULL' and author_active=1)
            )
        
            SELECT 
                cas_pubs.author_id,
                cas_pubs.author_name,
                cas_pubs.author_raw_name,
                author_department,
                author_position,
                author_is_corresponding,
                authors.author_role,
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                work_sustainable_dev_goal,
                work_is_open_access,
                work_cited_by_count

            FROM 
                cas_pubs

            LEFT JOIN
            authors 
            ON cas_pubs.author_id = authors.author_alexid

            GROUP BY 
                cas_pubs.author_id,
                cas_pubs.author_name,
                cas_pubs.author_raw_name,
                author_department,
                author_position,
                author_is_corresponding,
                authors.author_role,
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                work_sustainable_dev_goal,
                work_is_open_access,
                work_cited_by_count
                
             HAVING
                 work_publication_year = {FROM_YEAR}
            ORDER BY cas_pubs.author_name;
"""
    df = pd.read_sql_query(query, engine)
    if curators is True:
        df = df[df['author_role'] == 'Curator']
    df.to_csv(output_file, index=False)
    return df

def main():
    check_outfile_directory()
    engine = create_engine()
    if RESULTS_BY_INDIVIDUAL_AUTHOR is True:
        df = single_authors_to_df_csv(engine, OUTFILE)
    else:
        df = concat_authors_works_to_df_csv(engine, OUTFILE)      
if __name__ == "__main__":
    main()
