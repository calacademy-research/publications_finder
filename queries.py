"""
Module for querying the publications.comprehensive_global table and returning
publications/works for a certain period.
"""
import pandas as pd
from db_connection import DBConnection

# Creating the authors table
sql_create_table = """ CREATE TABLE IF NOT EXISTS authors (
                                        author_idx INT NOT NULL AUTO_INCREMENT,
                                        author_wikidata VARCHAR(45),
                                        author_orcid VARCHAR(45),
                                        author_alexid VARCHAR(40) NOT NULL,
                                        author_name TINYTEXT,
                                        author_raw_name TINYTEXT,
                                        author_role TINYTEXT,
                                        author_department VARCHAR(255),
                                        author_active INT,
                                        author_notes TINYTEXT,
                                        PRIMARY KEY (author_idX)
                                        );
                                        """
DBConnection.execute_query(sql_create_table)

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

# Pubs by cas authors during FY23
# from_date = '2022-07-01'
# to_date = '2023-06-30'

# sql = f""" WITH cas_pubs AS (
#             SELECT * FROM `publications`.`comprehensive_global_works` 
#             WHERE institution_name = 'California Academy of Sciences'
#             OR author_orcid in (SELECT author_orcid FROM authors where author_orcid is not NULL))

#             SELECT 
#                 work_id,
#                 work_doi,
#                 work_display_name,
#                 work_publication_date,
#                 work_publication_year,
#                 GROUP_CONCAT(DISTINCT author_name SEPARATOR ', ') AS authors_concatenated

#             FROM 
#                 cas_pubs

#             GROUP BY 
#                 work_id,
#                 work_doi,
#                 work_display_name,
#                 work_publication_date,
#                 work_publication_year
                
#             HAVING
#                 work_publication_date >= '{from_date}'
#                 AND work_publication_date <= '{to_date}'
                
#             ORDER BY authors_concatenated;
#             """

# result = DBConnection.execute_query(sql)
# df = pd.DataFrame(result)
# print(df[5])
