"""
Ingest publication data based on either affiliation or author information
(author info to be completed)
"""
# from crossref import Crossref
import sys
from openalex_ingest import OpenAlexIngest


#old and busted
# def main():
#     crossref = Crossref()
#     # Your main code goes here
#     crossref.author_search("Joe Russack")

# new hotness

def main(query_option=None):
    """
    Start ingesting based on option.
    
    Args:
    query_option (str): The option to search and ingest by. Either 'by_affiliation' or
                        'by_author_orcid'
    might change how this work later?
    """

    # use some kind of flag - either a config option or a command line argument to determine
    # whether we're going to re-pull the instutional level query to populate the database again.
    if query_option is None:
        print('Please specify by_affiliation or by_author')

    ingestor = OpenAlexIngest(query_option=query_option)
    ingestor.insert_works()
    # if (magical whatgever makese sense params go here):
    #     open_alex.populate_instutition()
    # if (query_by_research_assocaites):
    #     open_alex.query_by_author()
    print('records ingested')


if __name__ == "__main__":
    main()
