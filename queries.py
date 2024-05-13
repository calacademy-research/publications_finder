"""
Module for querying the publications.comprehensive_global table and returning
works for a certain period. The works are organized by either concatenated authors
or by individual authors. The individual authors option provides role and department information
for each author. The concatenated authors option does not. 
"""
import argparse
import os
import pandas as pd
import sqlalchemy as sa
import yaml
from db_connection import DBConnection
from config import Config

# Set config variables
# config = Config()
# FROM_YEAR = config.get_int("years", "from_year")
# TO_YEAR = config.get_int("years", "to_year")
# RESULTS_BY_INDIVIDUAL_AUTHOR = config.get_boolean("query_results", "single_authors")
# CURATORS = config.get_boolean("query_results", "curators")
# SUSTAINABILITY_GOALS = config.get_boolean("query_results", "sustainable_goals")
# DEPARTMENT=config.get_string("query_results", "department")
# JOURNAL_INFO=config.get_boolean("query_results", "journal_info")
# OPEN_ACCESS=config.get_boolean("query_results", "open_access_info")

# # Assembling the OUTFILE path
# base_path = 'generated_csvs/'
# options = []

# if CURATORS is True:
#     options.append('curators')
# if DEPARTMENT is not None:
#     options.append(f'{DEPARTMENT}')
# if FROM_YEAR:
#     options.append(str(FROM_YEAR))
# if TO_YEAR:
#     options.append(str(TO_YEAR))

# filters = f'{"_".join(map(str,options))}'
# OUTFILE = base_path + filters
# print ('Publications csv saved to: ', OUTFILE)

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

def concat_authors_works_to_df_csv(engine,
                                   from_year,
                                   to_year,
                                   output_file):
    """
    Generate a csv of CAS works with all authors concatenated into one field.
    This is useful for counting all works for a time interval since
    there will not be duplicate works.

    Args:
    engine (sqlalchemy engine instance)
    output_file (str): csv path of query results. Default is OUTFILE
    goals (bool): whether or not to sort papers by sustainability goals and display counts of each goal.

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
                 work_publication_year >= {from_year}
                 AND work_publication_year <= {to_year}
            ORDER BY authors_concatenated;
        """
    df = pd.read_sql_query(query, engine)
    df['work_sustainable_dev_goal'] = df['work_sustainable_dev_goal'].str.replace('-1', 'Uncategorized')
    df.to_csv(output_file, index=False)
    return df

def single_authors_to_df_csv(engine,
                             curators,
                             department,
                             from_year,
                             to_year,
                             output_file):
    """
    Generate a csv of CAS works by individual author.
    This is useful for filtering by individual role, department, etc.
    Not useful for total tally of works because there will be duplicate works.

    Args:
    engine (sqlalchemy engine instance)
    output_file (str): csv path of query results. Default is OUTFILE.
    curators (bool): Filter for curators vs everyone else.
    department (str): Filter to only get results for certain department.

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
                 work_publication_year >= {from_year}
                 AND work_publication_year <= {to_year}

            ORDER BY cas_pubs.author_name;
"""
    df = pd.read_sql_query(query, engine)
    if curators is True:
        df = df[df['author_role'] == 'Curator']
    if curators is False:
        df = df[df['author_role'] != 'Curator']
    if department is not None:
        df = df[df['author_department'] == department]
        df = combine_authors(df)
        df = df.drop(columns=['author_name', 'author_raw_name',
                         'author_department','author_position',
                         'author_is_corresponding','author_role'])
    df.to_csv(output_file, index=False)
    return df

def combine_authors(df, publication_id_col='work_id',
                    author_name_col='author_raw_name',
                    combined_authors_col='combined_authors'):
    """
    Combine multiple authors for each publication into a single column.

    Args:
    - df: DataFrame containing CAS publications and authors.
    - publication_id_col: The column containing the publication IDs, openAlex work_ids in this case.
    - author_name_col: The column containing the author names. Using raw names in this case.
    - combined_authors_col: New column to store the combined author names.

    Returns:
    - A new DataFrame with combined author names for each publication.
    """
    # Group by publication ID and combine author names by
    # concatenating the author names for each group into a single string, separated by commas.
    combined = df.groupby(publication_id_col)[author_name_col].apply(lambda x: ', '.join(x)) \
        .reset_index(name=combined_authors_col)

    # Merge the combined authors back into the original DataFrame
    # and drop duplicates
    df_combined = df.merge(combined,
                           on=publication_id_col,
                           how='left') \
        .drop_duplicates(publication_id_col)

    return df_combined

def return_journal_stats(df):
    """
    Returns a dataframe of journal publishers and titles sorted in ascending order of how many CAS works
    were published in each combination of publisher + journal

    Args:
    df (pandas DataFrame)

    Returns:
    DataFrame sorted in ascending count order
    """
    df = df.groupby(['work_publisher', 'work_journal']).size().reset_index(name="count").sort_values("count",ascending=False)
    return df

def return_sustainability_goal_stats(df):
    """
    Returns a dataframe of sustainability goals sorted in ascending order of how many CAS works
    were identified under each goal.

    Args:
    df (pandas DataFrame)

    Returns:
    DataFrame sorted in ascending count order
    """
    df = df.sort_values('work_sustainable_dev_goal')
    goal_counts= df.groupby('work_sustainable_dev_goal').size().reset_index(name='counts')
    goal_counts = goal_counts.sort_values('counts', ascending=False)
    return goal_counts

def return_open_access_stats(df):
    """
    Returns a dataframe of open access stats (open=1, closed=0)

    Args:
    df (pandas DataFrame)

    Returns:
    prints value counts
    """
    oa_stats = df['work_is_open_access'].value_counts()
    print(oa_stats)
    return oa_stats

def parse_args():
    """Parse the command line arguments"""
    # action='store_true' means they are false by default and become true if included in the command line.
    parser = argparse.ArgumentParser(description='Settings for query results.')
    parser.add_argument('--from_year', type=int, default=2022, help='Start year')
    parser.add_argument('--to_year', type=int, default=2022, help='End year')
    parser.add_argument('--single_authors', action='store_true')
    parser.add_argument('--curators', action='store_true', help='Include curators in the results')
    parser.add_argument('--sustainable_goals', action='store_true', help='Filter for sustainable goals')
    parser.add_argument('--department', type=str, choices=[
        'Anthropology', 'Aquarium', 'Botany', 'Center for Biodiversity and Community Science',
        'Center for Comparative Genomics', 'Center for Exploration and Travel Health',
        'Coral Regeneration Lab', 'Education', 'Entomology', 'Herpetology', 'Ichthyology',
        'iNaturalist', 'Invertebrate Zoology and Geology', 'Microbiology',
        'Ornithology and Mammalogy', 'Planetarium', 'Scientific Computing'
    ])
    parser.add_argument('--journal_info', action='store_true')
    parser.add_argument('--open_access_info', action='store_true')
    # parser.add_argument('--output_file', type=str)
    return parser.parse_args()

def assemble_outfile_path(args):
    """Assemble outfile path based on command line arguments."""
    base_path = 'generated_csvs/'
    options = []

    if args.curators:
        options.append('curators')
    if args.department:
        options.append(args.department)
    if args.from_year:
        options.append(str(args.from_year))
    if args.to_year:
        options.append(str(args.to_year))

    filters = "_".join(options)
    outfile = f"{base_path}{filters}"
    return outfile


def main():
    """main function to run query with the specified command line settings."""
    args = parse_args()

    outfile = assemble_outfile_path(args)
    print('Publications csv saved to:', outfile + '.csv')
    check_outfile_directory()

    engine = create_engine()
    # if RESULTS_BY_INDIVIDUAL_AUTHOR is True:
    if args.single_authors:
        df = single_authors_to_df_csv(engine,
                                #  curators=CURATORS,
                                #  department=DEPARTMENT,
                                #  output_file=OUTFILE)
                                curators=args.curators,
                                from_year=args.from_year,
                                to_year=args.to_year,
                                department=args.department,
                                output_file=outfile + '.csv')
    else:
        df = concat_authors_works_to_df_csv(engine,
                                    #    output_file=OUTFILE)
                                    from_year=args.from_year,
                                    to_year=args.to_year,
                                    output_file=outfile + '.csv')
    # if JOURNAL_INFO is True:
    if args.journal_info:
        journal_info = return_journal_stats(df)
        # path_for_journal_csv = OUTFILE + '_journal_info'
        path_for_journal_csv = outfile + '_journal_info'
        journal_info.to_csv(path_for_journal_csv + '.csv', index=False)
        print('Journal Info:\n', journal_info)
        print('Journal Info saved to: ', path_for_journal_csv)

    # if SUSTAINABILITY_GOALS is True:
    if args.sustainable_goals:
        goal_info = return_sustainability_goal_stats(df)
        # path_for_goal_csv = OUTFILE + '_goal_info'
        path_for_goal_csv = outfile + '_goal_info'
        goal_info.to_csv(path_for_goal_csv + '.csv', index=False)
        print('Sustainability goal counts: \n', goal_info)
        print('Sustainability Goal Info saved to: ', path_for_goal_csv)

    # if OPEN_ACCESS is True:
    if args.open_access_info:
        oa_stats = return_open_access_stats(df)
        print(oa_stats)

if __name__ == "__main__":
    main()
