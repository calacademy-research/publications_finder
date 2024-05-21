"""
Ingest publication data based on affiliation and author orcid.
"""
# from crossref import Crossref
# import sys
from openalex_ingest import OpenAlexIngest

def main():
    """
    Start ingesting based on affiliation and then author orcid.
    
    Args:
    query_option (str): The option to search and ingest by. Either 'by_affiliation' or
                        'by_author_orcid'
    """
    ingestor = OpenAlexIngest(query_option='by_affiliation')
    print("Ingesting records by affiliation...")
    ingestor.insert_works()
    print("Removing works by authors where author metadata is incorrect...")
    ingestor.remove_works()
    print('Done. Records ingested by affiliation')

    ingestor = OpenAlexIngest(query_option='by_author_orcid')
    print("Ingesting records by author ORCID...")
    ingestor.insert_works()
    print("Removing works by authors where author metadata is incorrect...")
    ingestor.remove_works()
    print('Done. Records ingested by author ORCID')

if __name__ == "__main__":
    main()
