"""Class for getting API data and structuring it for ingestion."""
import requests
from config import Config
# from orcid_list import cas_orcid_list


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
        self.email = self.config.get_string("alex_param", 'email')

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
        email = email or self.email
        endpoint = 'https://api.openalex.org/works'
        filters = [f'institutions.ror:{ror}']

        if from_date:
            filters.append(f'from_publication_date:{from_date}')
        if to_date:
            filters.append(f'to_publication_date:{to_date}')

        filter_param = f'filter={",".join(filters)}'
        mailto_param = f'&mailto={email}' if email else ''
        url = f'{endpoint}?{filter_param}{mailto_param}'
        print(f"Url for API call: {url}")

        return url

    def retrieve_author_id(self,
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
        Nothing. Just prints found alexids and orcids for the searched name.
        Have to pick which ids to select for search by orcid in build_author_words().  
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
        # Potentially use affiliations to filter for CAS only authors and then return that ORCID?
        # Might cause issues if CAS not in current affiliations for an author though.
        return 1

    @staticmethod
    def chunk_list(lst, chunk_size):
        """Yield successive chunks from a list."""
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    def _build_author_works_url(self,
                                author_orcid = None,
                                from_date=None, # add dates to config.ini?
                                to_date=None,
                                email=None,
                                chunk_size=30): # add to config.ini?
        ''' Build URLS for API calls to retrieve all works for the provided author details.  

        Args:
        author_orcid (list): Either a single ORCID (as a list of length 1) for the author to search on, or a list of
                                    author orcids to search on. note: you can't query works on openalex
                                    with just author name strings (bc of name ambiguity).
                                    Lookup ORCIDS for authors using retrieve_author_id(). 
        from_date (str): Format: 'YYYY-MM-DD'. Will retrieve all works on or after this date.
        to_date (str): Format: 'YYYY-MM-DD'. Will retrieve all works up to or on this date.
        email (str): Provide email address in order to get into the polite pool for API requests
        chunk_size (int): The number of orcids to batch together into one request. The max per request is 100,
                          but the url is too long for the server with 100 orcids + the select parameters (max=4094),
                          so changed the default to 30, which seems to be the smallest number that works.
                          Could also experiment with getting rid of the select parameters later.
                          
        Returns:
        List of complete URLS for API calls.
        '''
        endpoint = 'https://api.openalex.org/works'
        # https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/filter-entity-lists
        # https://blog.ourresearch.org/fetch-multiple-dois-in-one-openalex-api-request/
        # can pipe together up to 100 ORCIDS in one call. use per-page=100
        email = email or self.email
        urls = []

        for orcid_chunk in self.chunk_list(author_orcid, chunk_size):
            filters = [
                f'authorships.author.orcid:{"|".join(orcid_chunk) if isinstance(author_orcid, list) else author_orcid}',
                *(f'from_publication_date:{from_date}' if from_date else []),
                *(f'to_publication_date:{to_date}' if to_date else [])
            ]
            filter_param = f'filter={",".join(filters)}'
            mailto_param = f'&mailto={email}' if email else ''
            url = f'{endpoint}?{filter_param}{mailto_param}&per-page={chunk_size}'
            urls.append(url)

        return urls
    
    @staticmethod
    def _page_thru_all_pubs(url_in):
        ''' Page through API results, which needs to happen when there > 25 results.

        Args: 
        url_in: url from `build_institution_works_url()`

        Returns:
        List of json response data from API call
        '''
        cursor = '*'

        # modify these to retrieve other fields as needed.
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
            'sustainable_development_goals'
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
        List of dictionaries where each entry is a work by an author at certain institution.
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
            
                    if authorship['institutions']:
                        for institution in authorship['institutions']:
                            institution_id = institution['id']
                            institution_name = institution['display_name'] 
                            institution_country_code = institution['country_code']
                    else:
                        institution_id = "Not provided in metadata from publication source"
                        institution_name = "Not provided in metadata from publication source"
                        institution_country_code = "Not provided in metadata from publication source"
                    try:
                        work_publisher = work['primary_location']['source'].get('host_organization_name', -1)
                        work_journal = work['primary_location']['source'].get('display_name', -1)
                    except (TypeError, AttributeError, KeyError):
                        work_publisher = -1
                        work_journal = -1
                    data.append({
                        'work_id': work['id'],
                        'work_doi': work['ids'].get('doi', -1),
                        'work_title': work['title'],
                        'work_display_name': work['display_name'],
                        'work_publisher': work_publisher,
                        'work_journal': work_journal,
                        'work_publication_year': work['publication_year'],
                        'work_publication_date': work['publication_date'],
                        'work_sustainable_dev_goal': work['sustainable_development_goals'][0]['display_name']
                                                    if work['sustainable_development_goals'] else -1,
                        'author_id': author_id,
                        'author_orcid': author_orcid,
                        'author_name': author_name,
                        'author_raw_name': author_raw_name,
                        # 'author_position': author_position,
                        'institution_id': institution_id,
                        'institution_name': institution_name,
                        'institution_country_code': institution_country_code
                    })
        return data

    def query_by_affiliation(self):
        """
        Return structured data from affiliation api query to insert into
        MySQL table in openalex_ingest.py.
        Uses the internal functions _build_institution_works_url() and 
        _structure_works().
        """
        url = self._build_institution_works_url()
        works = self._page_thru_all_pubs(url)
        print("Structuring records for ingestion...")
        structured_data = self._structure_works(works)
        return structured_data


    def query_by_author(self, orcid=None):
        """
        Return structured data from author orcid api query
        to insert into MySQL table in openalex_ingest.py.
        Uses the internal functions _build_institution_works_url() and 
        _structure_works()

        Args:
        orcid (list): Either a single orcid (as list of length 1) or list of orcids to query by.
                The default is None, which triggers cas_orcid_list,
                which represents all orcids associated with CAS researchers. 
                Changed to only search through orcids that are known to be missing from results!
        """
        # cas_orcid_list comes from orcid_list.py
        if orcid is None:
            # orcid = cas_orcid_list
            orcid = self.config.get_list("orcids", "orcid_list")
        urls = self._build_author_works_url(orcid)
        # assembling all works from multiples urls:
        all_works = []
        for i, url in enumerate(urls):
            print(f"Working on url {i+1} out of {len(urls)}")
            works = self._page_thru_all_pubs(url)
            all_works.extend(works)
    
        print(f'Total works collected from all URLs: {len(all_works)}')
        print("Structuring records for ingestion...")
        structured_data = self._structure_works(all_works)
        return structured_data
          
# api = OpenAlex()
# author_url = api._build_author_works_url('joseph', 'russack', 'mabarca@calacademy.org')
# print(author_url)

# api.retrieve_author_id('joseph', 'russack')
# api.query_by_author()
