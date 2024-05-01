"""
Module for updating CAS author table based on a downloaded tsv. 
"""
import pandas as pd
import sqlalchemy as sa
import subprocess
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

def create_author_table():
    """Create the authors table if it hasn't been created yet"""
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

def docker_cp(container_id='e01619ab8ab11bd0b60888227f070e0cf51673895e69b6083a143b1c67816603',
              source_path='data/cas_authorids_v8.tsv',
              destination_path='/var/lib/mysql-files/'):
    """
    Copy files between a Docker container and the host using the 'docker cp' command.
    Need this in order to read from a tsv since MySQL in Docker isn't allowed to read
    from local directories. 

    :param container_id: ID or name of the Docker container.
    :param source_path: Path of the source file or directory.
    :param destination_path: Path of the destination file or directory on the host.
    """
    command = f"docker cp {source_path} {container_id}:{destination_path}"
    try:
        result = subprocess.run(command, shell=True, check=True,
                                text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        print("Copy successful:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error occurred:", e.stderr)

def load_author_data(tsv_filepath='/var/lib/mysql-files/cas_authorids_v8.tsv'):
    """
    Load data from tsv of authors that is stored in docker container.
    Will overwrite table with the data in tsv.  
    Note on how to read from files in Docker SQL: 
    run `SHOW VARIABLES LIKE 'secure_file_priv';` to get the path where files are allowed to be read from
    will probably be: /var/lib/mysql-files/
    then run `docker cp /path/to/local/file.csv my_mysql_container:/var/lib/mysql-files/``
    change path to your local file and the name/id of the docker container holding your sql db,
    and path if it's different than above

    Args:
    tsv_filepath (str): local path where tsv of author info is stored.
    """

    # future updates will prob just need on duplicate key update
    sql_load_data = f""" LOAD DATA INFILE '{tsv_filepath}'
                INTO TABLE authors
                FIELDS TERMINATED BY '\t'
                LINES TERMINATED BY '\n'
                IGNORE 1 LINES
                (author_wikidata,
                author_orcid,
                author_alexid,
                author_name,
                author_raw_name,
                author_role,
                author_department,
                author_active,
                author_notes);
    """
    DBConnection.execute_query(sql_load_data)

def update_author_table():
    create_author_table()
    docker_cp()
    load_author_data()
    print("Author table updated.")


update_author_table()
