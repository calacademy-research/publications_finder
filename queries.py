"""
Module for querying the comprehensive_global_works table and returning
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

# wrap this all up into a class?
def check_outfile_directory(dir_path='./generated_csvs2/'):
    """Create directory for generated csvs if it doesn't exist."""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    # add part to clear files in directory otherwise

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
    from_year (int): start year (inclusive)
    to_year (int): end year (inclusive)
    output_file (str): csv path to query results.


    Returns:
    df: (pandas df) df of query results
    Also saves df as csv into output_file path. 
    """
    query = f"""
        WITH cas_pubs AS (
            SELECT * FROM `works`.`comprehensive_global_works` 
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
                work_type,
                work_topic,
                work_is_open_access,
                work_cited_by_count,
                work_created_date,
                work_updated_date

            FROM 
                cas_pubs

            GROUP BY 
                work_id,
                work_doi,
                work_display_name,
                work_type,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                work_sustainable_dev_goal,
                work_type,
                work_topic,
                work_is_open_access,
                work_cited_by_count,
                work_created_date,
                work_updated_date

             HAVING
                 work_publication_year >= {from_year}
                 AND work_publication_year <= {to_year}
            ORDER BY authors_concatenated;
        """
    df = pd.read_sql_query(query, engine)
    df['work_sustainable_dev_goal'] = df['work_sustainable_dev_goal'].str.replace('-1', 'Uncategorized')
    df['work_topic'] = df['work_topic'].str.replace('-1', 'Uncategorized')
    df['work_type'] = df['work_type'].str.replace('-1', 'Uncategorized')
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
    curators (bool): Filter for curators vs everyone else.
    department (str): Filter to only get results for certain department.
    from_year (int): start year (inclusive)
    to_year (int): end year (inclusive)
    output_file (str): csv path to query results.

    Returns:
    df: (pandas df) dataframe of query results.
    Also saves csv of dataframe to output_file path.
    """
  
    query = f"""
        WITH cas_pubs AS (
            SELECT * FROM `works`.`comprehensive_global_works` 
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
                work_type,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                work_sustainable_dev_goal,
                work_topic,
                work_type,
                work_is_open_access,
                work_cited_by_count,
                work_created_date,
                work_updated_date

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
                work_type,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                work_sustainable_dev_goal,
                work_topic,
                work_type,
                work_is_open_access,
                work_cited_by_count,
                work_created_date,
                work_updated_date
                
             HAVING
                 work_publication_year >= {from_year}
                 AND work_publication_year <= {to_year}

            ORDER BY cas_pubs.author_name;
"""
    df = pd.read_sql_query(query, engine)
    df['work_sustainable_dev_goal'] = df['work_sustainable_dev_goal'].str.replace('-1', 'Uncategorized')
    df['work_topic'] = df['work_topic'].str.replace('-1', 'Uncategorized')
    df['work_type'] = df['work_type'].replace('-1', 'Uncategorized')

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
    oa_values = df['work_is_open_access'].value_counts()
    num_open = len(df[df['work_is_open_access'] == '1'])
    oa_percent = num_open / len(df) * 100
    return oa_values, oa_percent

def parse_args():
    """Parse the command line arguments"""
    # action='store_true' means they are false by default and become true if included in the command line.
    parser = argparse.ArgumentParser(description='Settings for query results.')
    parser.add_argument('--from_year', type=int, default=2022, help='Start year')
    parser.add_argument('--to_year', type=int, default=2022, help='End year')
    parser.add_argument('--single_authors', action='store_true', help='Trigger by curator or by department results.')
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
    
    return parser.parse_args()

def assemble_outfile_path(args):
    """Assemble outfile path based on command line arguments."""
    base_path = 'generated_csvs2/'
    options = []

    if args.curators is True:
        options.append('curators')
    if args.department:
        options.append(args.department.lower())
    if args.from_year:
        options.append(str(args.from_year))
    if args.to_year:
        options.append(str(args.to_year))

    filters = "_".join(options)
    outfile = f"{base_path}works_{filters}"
    return outfile

def add_filters_to_email_template(args, email_template_path):
    """Add text to email body that describes the filters applied."""
    with open (email_template_path, 'w') as file:
        # Subject
        file.write('CAS Works csv\n')

        # Body
        file.write('\nDescription of report parameters:\n')
        if args.curators is True:
            file.write('Curator filter applied.\n')
        if args.department:
            file.write(f'Results for {args.department} applied.\n')
        if args.from_year:
            file.write(f'Results from {args.from_year} applied.\n')
        if args.to_year:
            file.write(f'Results until {args.to_year} applied.\n')
        if args.journal_info:
            file.write('Results include a csv of journal information.\n')
        if args.sustainable_goals:
            file.write('Results include a csv of sustainability goals.\n')
        if args.open_access_info:
            file.write('Results include a csv of open access information.\n')
        

        


def main():
    """main function to run query with the specified command line settings."""
    args = parse_args()

    outfile = assemble_outfile_path(args)
    print('Publications csv saved to:', outfile + '.csv')
    check_outfile_directory()
    add_filters_to_email_template(args, 'email_template.txt')

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
        oa_values, oa_percent = return_open_access_stats(df)
        print(oa_values)
        print(f'Percent open access: {oa_percent:.2f}%')

if __name__ == "__main__":
    main()
