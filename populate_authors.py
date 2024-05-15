"""Populate author information from a spreadsheet"""
import argparse
from db_connection import DBConnection
import mysql.connector
import subprocess
import yaml

     
def load_data_into_container(config_file_path, local_sheet_path, docker_container_name):
    """Copy downloaded sheet to a Docker directory that MySQL can read from.
     
     Args:
     config_file_path(str): Local path where config file is
     local_file_path(str): Local path of spreadsheet to load author data from.
     docker_container_name(str): Docker container name/id

     Returns:
     Docker filepath for spreadsheet
     """
    # Connect to MySQL database
    with open(config_file_path, 'r') as file:
        config = yaml.safe_load(file)

        database_url = config['database_url']
        database_password = config['database_password']
        database_user = config['database_user']
        database_name = config['database_name']

    conn = mysql.connector.connect(
        host=database_url,
        user=database_user,
        password=database_password,
        database=database_name
    )
    cursor = conn.cursor()

    # Check the allowed path for loading files
    cursor.execute("SHOW VARIABLES LIKE 'secure_file_priv';")
    secure_file_priv = cursor.fetchone()[1]
    print(f"Secure file priv location: {secure_file_priv}")

    # Define the path inside the Docker container
    container_file_path = secure_file_priv + local_sheet_path.split('/')[-1]

    # Copy file to Docker container
    docker_cp_command = f"docker cp {local_sheet_path} {docker_container_name}:{container_file_path}"
    subprocess.run(docker_cp_command, shell=True, check=True)
    print(f"File copied to Docker container: {container_file_path}")

    return container_file_path


def populate_authors_table(spreadsheet_path,
                        load_data=False,
                        update_data=True):
        """Create authors table if it doesn't exist. Populate with author info from spreadsheet.

        Args:
        spreadsheet_path(str): Path to spreadsheet that contains author info.
        load_data(bool): Load all data from spreadsheet into authors table. Good for first creation of authors table.
        update_data(bool): Update only the modified records from the spreadsheet into the author table, 
                           and/or add only newly entered authors from spreadsheet.

        """
        # Creating the authors table
        sql_create_table = """ CREATE TABLE IF NOT EXISTS authors (
                                            author_wikidata VARCHAR(45),
                                            author_orcid VARCHAR(45),
                                            author_alexid VARCHAR(40) NOT NULL,
                                            author_name TINYTEXT,
                                            author_raw_name TINYTEXT,
                                            author_role TINYTEXT,
                                            author_department VARCHAR(255),
                                            author_active INT,
                                            author_notes TINYTEXT,
                                            PRIMARY KEY (author_alexid)
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

        if load_data is True: # Load all data from sheet
            sql_load_data = f"""LOAD DATA INFILE {spreadsheet_path}
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

        if update_data is True: # Just update records or add new ones from sheet.
            sql_create_temp = f""""CREATE TEMPORARY TABLE temp_authors (
                                    author_wikidata VARCHAR(45),
                                    author_orcid VARCHAR(45),
                                    author_alexid VARCHAR(40) NOT NULL,
                                    author_name TINYTEXT,
                                    author_raw_name TINYTEXT,
                                    author_role TINYTEXT,
                                    author_department VARCHAR(255),
                                    author_active INT,
                                    author_notes TINYTEXT,
                                    PRIMARY KEY (author_alexid)
                            );
                            LOAD DATA INFILE {spreadsheet_path}
                            INTO TABLE temp_authors
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
            DBConnection.execute_query(sql_create_temp)

            sql_update_authors = """INSERT INTO authors (author_wikidata, author_orcid,
                                        author_alexid, author_name, author_raw_name,
                                        author_role, author_department,
                                        author_active, author_notes)
                                    SELECT author_wikidata, author_orcid,
                                        author_alexid, author_name, author_raw_name,
                                        author_role, author_department,
                                        author_active, author_notes
                                    FROM temp_authors
                                    ON DUPLICATE KEY UPDATE
                                        author_orcid = VALUES(author_orcid),
                                        author_alexid = VALUES(author_alexid),
                                        author_name = VALUES(author_name),
                                        author_raw_name = VALUES(author_raw_name),
                                        author_role = VALUES(author_role),
                                        author_department = VALUES(author_department),
                                        author_active = VALUES(author_active),
                                        author_notes = VALUES(author_notes);

                                    DROP TEMPORARY TABLE temp_authors;
                                """
            DBConnection.execute_query(sql_update_authors)

def parse_args():
    """Parse the command line arguments"""
    # action='store_true' means they are false by default and become true if included in the command line.
    parser = argparse.ArgumentParser(description='Options for populating the authors table.')
    parser.add_argument('--config_path', type=str, help='Filepath for config file.')
    parser.add_argument('--local_sheet_path', type=str, help='Path to locally downloaded spreadsheet.')
    parser.add_argument('--container_name', type=str, help='Docker container name.')
    parser.add_argument('--load_data', action='store_true', help='Load all data from spreadsheet')
    parser.add_argument('--update_data', action='store_true', help='Update fields based on spreadsheet updates.')

    return parser.parse_args()


def main():
     args = parse_args()
     container_file_path = load_data_into_container(args.config_path, args.local_sheet_path, args.container_name)

     populate_authors_table(container_file_path, load_data=args.load_data, update_data=args.updata_data)
     print("Authors table created and updated.")


if __name__ == "__main__":
     main()