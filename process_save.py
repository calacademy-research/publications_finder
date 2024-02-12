"""
Builds an API call with provided institution id,date range for results,
and email to be put into the polite request pool.
Pages through results, pickles them, and processes the results into a cleaned DataFrame. 
Saves the cleaned DataFrame for future data processing and plotting.
"""
import datetime
import os
import pickle
import requests
import pandas as pd


# ROR id for California Academy of Sciences
ROR = '02wb73912'

def build_institution_works_url(ror=ROR,
                                from_date=None,
                                to_date=None,
                                email=None):
    ''' Build URL for API call to retrieve all works where the provided 
    institution ROR is in at least one of the affiliations for a work. 

    Args:
    ror: The ROR id for the institution.
    from_date: String. Format: 'YYYY-MM-DD'. Will retrieve all works on or after this date.
    to_date: String. Format: 'YYYY-MM-DD'. Will retrieve all works up to or on this date.
    email: Provide email address in order to get into the polite pool for API requests

    Returns:
    Complete URL for API call
    
    '''
    endpoint = 'https://api.openalex.org/works'
    filters = [f'institutions.ror:{ror}']

    if from_date:
        filters.append(f'from_publication_date:{from_date}')
    if to_date:
        filters.append(f'to_publication_date:{to_date}')

    filter_param = f'filter={",".join(filters)}'
    mailto_param = f'&mailto={email}' if email else ''
    url =  f'{endpoint}?{filter_param}{mailto_param}'

    return url, {'from_date': from_date, 'to_date': to_date}

def page_thru_all_pubs (url_in):
    ''' Page through API results, which needs to happen when there > 25 results.

    Args: 
    url_in: url from `build_institution_works_url()`

    Returns:
    List of json response data from API call
    '''
    cursor = '*'

    select = ",".join((
        'id',
        'ids',
        'title',
        'display_name',
        'publication_year',
        'publication_date',
        'primary_location',
        'open_access',
        'authorships',
        'cited_by_count',
        'is_retracted',
        'is_paratext',
        'updated_date',
        'created_date',
    ))

    # loop through pages
    works = []
    loop_index = 0
    while cursor:
        # set cursor value and request page from OpenAlex
        url = f'{url_in}&select={select}&cursor={cursor}'
        page_with_results = requests.get(url).json()
        results = page_with_results['results']
        works.extend(results)

        # update cursor to meta.next_cursor
        cursor = page_with_results['meta']['next_cursor']
        loop_index += 1
        if loop_index in [5, 10, 20, 50, 100] or loop_index % 500 == 0:
            print(f'{loop_index} api requests made so far')
    print(f'done. made {loop_index} api requests. collected {len(works)} works')
    return works

def pickle_results(data, dates):
    ''' Pickle the API results to avoid calling again. 

    Args:
    data: list from `page_thru_all_pubs()`
    dates: dictionary returned from `build_institution_works_url()`

    Returns:
    Path where pickle file was saved.
    '''
    directory_name = './data'
    os.makedirs(directory_name, exist_ok=True)

    from_date, to_date = dates.get('from_date'), dates.get('to_date')
    date_part = ''
    if from_date and to_date:
        date_part = f"_from_{from_date}_to_{to_date}"
    elif from_date:
        date_part = f"_from_{from_date}"
    elif to_date:
        date_part = f"_to_{to_date}"

    file_name = f'cas_works{date_part}.pickle'
    full_path = os.path.join(directory_name, file_name)

    with open(full_path, 'wb') as outf:
        pickle.dump(data, outf, protocol=pickle.HIGHEST_PROTOCOL)
    print(f'Pickle saved here: {full_path}')

    return full_path

def works_to_df(path):
    '''
    Loop through API output to construct a pandas DataFrame

    Args:
    path: path to json output of API call from pickle file returned from pickle_results()

    Returns:
    pandas DataFrame of results.
    '''
    with open(path, 'rb') as f:
        works = pickle.load(f)

    data = []
    for work in works:
        for authorship in work['authorships']:
            if authorship:
                author = authorship['author']
                author_id = author['id'] if author else None
                author_orcid = author.get('orcid', -1) if author else None
                author_name = author['display_name'] if author else None
                author_raw_name = authorship['raw_author_name']if author else None
                author_position = authorship['author_position']
                for institution in authorship['institutions']:
                    if institution:
                        institution_id = institution['id']
                        institution_name = institution['display_name']
                        institution_country_code = institution['country_code']
                        data.append({
                            'work_id': work['id'],
                            'work_doi': work['ids'].get('doi', -1),
                            'work_title': work['title'],
                            'work_display_name': work['display_name'],
                            'work_publication_year': work['publication_year'],
                            'work_publication_date': work['publication_date'],
                            'author_id': author_id,
                            'author_orcid': author_orcid,
                            'author_name': author_name,
                            'author_raw_name': author_raw_name,
                            'author_position': author_position,
                            'institution_id': institution_id,
                            'institution_name': institution_name,
                            'institution_country_code': institution_country_code,
                        })
    df = pd.DataFrame(data)
    return df

def clean_up_df(df):
    """
    Clean up result DataFrame from works_to_df(). 
    The loop creates a new row for each author of a publication,
    so people from other institutions get included and CAS authors get exploded
    into multiple records for the same publication.

    Args:
    df: DataFrame returned from works_to_df()

    Returns:
    DataFrame with only the n unique pubs with a column of all CAS authors for that publication.
    """
    df_cas = df[df['institution_name'] == 'California Academy of Sciences']
    return combine_authors(df_cas)

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
    combined = df.groupby(publication_id_col)[author_name_col].apply(lambda x:', '.join(x))\
                                                            .reset_index(name=combined_authors_col)

    # Merge the combined authors back into the original DataFrame
    # and drop duplicates
    df_combined = df.merge(combined,
                            on=publication_id_col,
                            how='left')\
                            .drop_duplicates(publication_id_col)

    return df_combined

def save_df_to_tsv(df):
    "Save DataFrame results to tsv"
    date_str = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
    path = f"./data/cleaned_results_{date_str}.tsv"
    df.to_csv(path, sep='\t', index=False)
    print(f"DataFrame saved to {path}")

def main():
    url, dates = build_institution_works_url(ror=ROR
                                            #  from_date='2022-01-01', #FY23
                                            #  to_date='2022-12-31'
                                             )
    publications = page_thru_all_pubs(url)
    pickle_path = pickle_results(publications, dates)
    df = works_to_df(pickle_path)
    cleaned_df = clean_up_df(df)
    save_df_to_tsv(cleaned_df)

    print("Results processed and saved.")

if __name__ == "__main__":
    main()
