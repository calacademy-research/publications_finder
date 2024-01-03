import sys
from crossref import Crossref

def main():
    crossref = Crossref()
    # Your main code goes here
    crossref.author_search("Joe Russack")
if __name__ == "__main__":
    main()
