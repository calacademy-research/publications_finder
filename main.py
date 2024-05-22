"""
Ingest publication data based on affiliation and author orcid.
"""
# from crossref import Crossref
import argparse
from openalex_ingest import OpenAlexIngest

def parse_args():
    """Parse the command line arguments"""
    # action='store_true' means they are false by default and become true if included in the command line.
    parser = argparse.ArgumentParser(description='Settings for ingesting results.')
    parser.add_argument('--update_works',
                        action='store_true',
                        help='Update records based on OpenAlex updates.')
    
    return parser.parse_args()

def main():
    """
    Start ingesting based on affiliation and then author orcid.
    
    Args:
    query_option (str): The option to search and ingest by. Either 'by_affiliation' or
                        'by_author_orcid'
    """
    args = parse_args()

    if args.update_works is False:
        print("Working on works by author affiliation...")
        ingestor = OpenAlexIngest(query_option='by_affiliation')
        print("Ingesting records by affiliation...")
        ingestor.insert_works()
        print("Removing works by authors where author metadata is incorrect...")
        ingestor.remove_works()
        print('Done. Records ingested by affiliation')

        print("Working on works by author ORCID...")
        ingestor = OpenAlexIngest(query_option='by_author_orcid')
        print("Ingesting records by author ORCID...")
        ingestor.insert_works()
        print("Removing works by authors where author metadata is incorrect...")
        ingestor.remove_works()
        print('Done. Records ingested by author ORCID')

    if args.update_works is True:
        print("Updating works by author affiliation...")
        ingestor = OpenAlexIngest(query_option='by_affiliation')
        print("Updating records by affiliation...")
        ingestor.update_works()
        print("Removing works by authors where author metadata is incorrect...")
        ingestor.remove_works()
        print('Done. Records updated by affiliation')

        print("Updating works by author ORCID...")
        ingestor = OpenAlexIngest(query_option='by_author_orcid')
        print("Updating records by author ORCID...")
        ingestor.update_works()
        print("Removing works by authors where author metadata is incorrect...")
        ingestor.remove_works()
        print('Done. Records updated by author ORCID')


if __name__ == "__main__":
    main()
