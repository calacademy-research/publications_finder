import sys
from crossref import Crossref
from db_connection import DBConnection
from openalex_api import OpenAlex()

#old and busted
# def main():
#     crossref = Crossref()
#     # Your main code goes here
#     crossref.author_search("Joe Russack")

# new hotness

def main():
    from db_connection import DBConnection
    # use some kind of flag - either a config option or a command line argument to determine
    # whether we're going to re-pull the instutional level query to populate the database again.
    open_alex =OpenAlex()
    if (magical whatgever makese sense params go here):
        open_alex.populate_instutition()
    if (query_by_research_assocaites):
        open_alex.query_by_author()


if __name__ == "__main__":
    main()
