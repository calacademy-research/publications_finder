"""Class for getting API data and structuring it for ingestion."""
import requests
from config import Config


class OpenAlex:
    """
    Class to make API calls and restructure the results for ingestion into sql table.

    Consider adding some args as config parameters?
    dates, email

    ToDo: create _build_author_works_url and the rest of the pipeline for author search. 
    Ingest author works if the openAlexID of any works associated with this author doesnt
    already exist in the table. Have to use openAlexID and not DOI bc some pubs do not have
    a DOI. 
    """
    # we will keep this regardless
    def __init__(self):
        self.config = Config()
        self.ror = self.config.get_string("alex_param", "ROR").strip("'")

    def _build_institution_works_url(self,
                                    ror=None,
                                    from_date=None, # add dates to config.ini?
                                    to_date=None,
                                    email=None): # add email to config.ini?
        ''' Build URL for API call to retrieve all works where the provided 
        institution ROR is in at least one of the affiliations for a work. 

        Args:
        ror: The ROR id for the institution. Set in config.ini
        from_date: String. Format: 'YYYY-MM-DD'. Will retrieve all works on or after this date.
        to_date: String. Format: 'YYYY-MM-DD'. Will retrieve all works up to or on this date.
        email: Provide email address in order to get into the polite pool for API requests

        Returns:
        Complete URL for API call
        '''
        ror = ror or self.ror
        endpoint = 'https://api.openalex.org/works'
        filters = [f'institutions.ror:{ror}']

        if from_date:
            filters.append(f'from_publication_date:{from_date}')
        if to_date:
            filters.append(f'to_publication_date:{to_date}')

        filter_param = f'filter={",".join(filters)}'
        mailto_param = f'&mailto={email}' if email else ''
        url = f'{endpoint}?{filter_param}{mailto_param}'
        print(url)

        return url

    def retrieve_author_alexid(self,
                                author_first_name = None, # add to config.ini?
                                author_last_name = None, # add to config.ini?
                                email=None): # add email to config.ini?):
        ''' Retrieve unique alexid for an author by first name and last. 
        Intended for use only when an author's ORCID or alexid aren't already known.
        You can only search for works for an author by their unique id's, not by their names
        because of the ambiguity that causes.  

        Args:
        author_first_name (str): Author's first name.
        author_last_name (str): Author's last name.

        Returns:
        Author's alexid and orcid if they are available through the api. 
        '''
        endpoint = 'https://api.openalex.org/authors'

        author_full_name = []

        if author_first_name:
            author_full_name.append(author_first_name)

        if author_last_name: 
            author_full_name.append(author_last_name)

        filter_param = f'filter=display_name.search:{" ".join(author_full_name)}'
        mailto_param = f'&mailto={email}' if email else ''
        url = f'{endpoint}?{filter_param}{mailto_param}'
        print(url)

        response = requests.get(url)
        author_results = {}
        if response.status_code == 200:
            page_with_results = response.json()
            results = page_with_results.get('results',[])
            if results:
                for result in results:
                    author_id = result['id']
                    author_orcid = result['orcid']
                    institutions = []
                    if result['affiliations']:
                        for affiliation in result['affiliations']:
                            institution_name = affiliation['institution']['display_name']
                            institutions.append(institution_name)
                    author_display_name = result.get('display_name', '')
        
                    author_results['author_searched_name'] = " ".join(author_full_name)
                    author_results['author_display_name'] = author_display_name
                    author_results['author_id'] = author_id
                    author_results['author_orcid'] = author_orcid or 'not available'
                    author_results['institution_names'] = ", ".join(institutions) if institutions else 'not available'
        
            
        # print(author_results)
        print("Searched Author Name:", author_results['author_searched_name'])
        print("Displayed Author Name:", author_results['author_display_name'])
        print("Author AlexID:", author_results['author_id'])
        print("Author ORCID:", author_results['author_orcid'])
        print("Author Affiliations:", author_results['institution_names'])

        # return url

        

    def _build_author_works_url(self,
                                author_orcid = None, # add to config.ini?
                                from_date=None, # add dates to config.ini?
                                to_date=None,
                                email=None): # add email to config.ini?
        ''' Build URL for API call to retrieve all works for the provided author details.  

        Args:
        author_first_name (str): Author's first name.
        author_last_name (str): Author's last name.
        from_date (str): Format: 'YYYY-MM-DD'. Will retrieve all works on or after this date.
        to_date (str): Format: 'YYYY-MM-DD'. Will retrieve all works up to or on this date.
        email (str): Provide email address in order to get into the polite pool for API requests

        Returns:
        Complete URL for API call
        '''
        endpoint = 'https://api.openalex.org/works'
        
        filters = []
        if author_orcid: 
            filters.append(f'{author_orcid}')
        if from_date:
            filters.append(f'from_publication_date:{from_date}')
        if to_date:
            filters.append(f'to_publication_date:{to_date}')

        filter_param = f'filter={",".join(filters)}'
        mailto_param = f'&mailto={email}' if email else ''
        url = f'{endpoint}?{filter_param}{mailto_param}'
        print(url)

        return url
    
    @staticmethod
    def _page_thru_all_pubs(url_in):
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
            response = requests.get(url)
            if response.status_code == 200:
                page_with_results = response.json()
                results = page_with_results.get('results',[])
                works.extend(results)

                # update cursor to meta.next_cursor
                cursor = page_with_results['meta']['next_cursor']
                loop_index += 1
                if loop_index in [5, 10, 20, 50, 100] or loop_index % 500 == 0:
                    print(f'{loop_index} api requests made so far')
            else:
                print(f'API request failed with status code {response.status_code}')
                break
        print(f'done. made {loop_index} api requests. collected {len(works)} works')
        return works

    @staticmethod
    def _structure_works(works):
        '''
        Loop through API output to structure it for MySQL table.

        Args:
        works: json output of API call

        Returns:
        pandas DataFrame of results.
        '''
        data = []
        for work in works:
            for authorship in work['authorships']:
                if authorship:
                    author = authorship['author']
                    author_id = author['id'] if author else None
                    author_orcid = author.get('orcid', -1) if author else None
                    author_name = author['display_name'] if author else None
                    author_raw_name = authorship['raw_author_name'] if author else None
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
        return data

    def query_by_affiliation(self):
        """
        Return structured data to insert into MySQL table in openalex_ingest.py
        Uses the internal functions _build_institution_works_url() and 
        _structure_works()
        
        """
        url = self._build_institution_works_url(email='mabarca@calacademy.org')
        works = self._page_thru_all_pubs(url)
        structured_data = self._structure_works(works)
        return structured_data


    def query_by_author(self):
        """to be completed"""
    # need _build_author_url
    # add args to search by name + orcid
              
api = OpenAlex()
# author_url = api._build_author_works_url('joseph', 'russack', 'mabarca@calacademy.org')
# print(author_url)

api.retrieve_author_alexid('joseph', 'russack')
