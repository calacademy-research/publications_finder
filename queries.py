"""
Module for querying the publications.comprehensive_global table and returning
publications/works for a certain period.
"""
import pandas as pd
import sqlalchemy as sa
import yaml
from db_connection import DBConnection

# Load the YAML file
with open('db_connection.yml', 'r') as file:
    config = yaml.safe_load(file)

database_url = config['database_url']
database_port = config['database_port']
database_password = config['database_password']
database_user = config['database_user']
database_name = config['database_name']

# Construct the connection string for MySQL using pymysql
connection_string = f"mysql+pymysql://{database_user}:{database_password}@{database_url}:{database_port}/{database_name}"

# Create SQL engine
engine = sa.create_engine(connection_string)

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

# Pubs by cas authors during FY23
# from_date = '2022-07-01'
# to_date = '2023-06-30'

# -- Use a CTE that filters works based on affiliation = CAS or author has an orcid that is currently associated with CAS
# sql = f""" 
# WITH cas_pubs AS (
#             SELECT * FROM `publications`.`comprehensive_global_works_v2` 
#              WHERE institution_name = 'California Academy of Sciences'
#              OR author_orcid in (SELECT author_orcid FROM authors where author_orcid != 'NULL' and author_active=1)
#             )
#             SELECT 
#                 work_id,
#                 work_doi,
#                 work_display_name,
#                 work_publication_date,
#                 work_publication_year,
#                 work_publisher,
#                 work_journal,
#                 GROUP_CONCAT(DISTINCT author_name SEPARATOR ', ') AS authors_concatenated,
#                 work_sustainable_dev_goal

#             FROM 
#                 cas_pubs

#             GROUP BY 
#                 work_id,
#                 work_doi,
#                 work_display_name,
#                 work_publication_date,
#                 work_publication_year,
#                 work_publisher,
#                 work_journal,
#                 work_sustainable_dev_goal
                
#              HAVING
#                  work_publication_year = '2022'   

#             ORDER BY authors_concatenated;
#            """

# result = DBConnection.execute_query(sql)
# df = pd.DataFrame(result)
# print(df[5])

# for authors csv
sql = """
SELECT 
    author_id,
    author_name,
    author_raw_name,
    author_position,
    author_is_corresponding,
    work_id,
    work_doi,
    work_display_name,
    work_publication_date,
    work_publication_year,
    work_publisher,
    work_journal,
    work_sustainable_dev_goal
FROM 
    (SELECT * FROM publications.comprehensive_global_works_v3 
    WHERE institution_name = 'California Academy of Sciences'
    OR author_orcid IN (SELECT author_orcid FROM authors WHERE author_orcid != 'NULL' AND author_active = 1)
    ) AS cas_pubs
GROUP BY 
    author_id,
    author_name,
    author_raw_name,
    author_position,
    author_is_corresponding,
    work_id,
    work_doi,
    work_display_name,
    work_publication_date,
    work_publication_year,
    work_publisher,
    work_journal,
    work_sustainable_dev_goal
HAVING work_publication_year = '2022'
ORDER BY author_name
"""

# Execute the query
df = pd.read_sql_query(sql, engine)

# Save to CSV
df.to_csv('./author_works_2022.csv', index=False)
