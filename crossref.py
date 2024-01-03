import logging
import requests
import urllib
import time
from utils_mixin import Utils

class RetriesExceededException(Exception):
    pass


class Crossref(Utils):
    # https://api.crossref.org/works?query.author=Joe+Russack
    # https://api.crossref.org//works?query.author=Joe+Russack
    # https://api.crossref.org/works?query.author=Joe+Russack&filter=orcid:0000-0003-1910-4865
    def __init__(self):
        super().__init__()
        self.crossref_url = f'https://api.crossref.org'
        self.query_url = self.crossref_url + '/works?query'
        self.author_url = self.query_url + '.author='
        self.headers = {
            'User-Agent': 'development; mailto:jrussack@calacademy.org',
        }

    def author_search(self, author):
        url = self.author_url + author.replace(" ", "+")
        logging.info(f"Querying: {url}")
        total_results = 0
        done = False
        total_items_processed = 0
        cursor = "*"
        logging.info(f"Searching author:{author}")

        while not done:
            try:
                cursor, total_results, items_processed = self._download_chunk(url, cursor)
                total_items_processed += items_processed
                logging.info("Continuing...")
            except ConnectionError:
                if total_items_processed >= total_results:
                    done = True
                    logging.info("Done.")
                else:
                    logging.info("retrying....")
            except RetriesExceededException as rex:
                logging.info(f"Retries exceeded: {rex}, aborting.")
                return

    def _download_chunk(self, url, cursor, retries=0):
        max_retries = 3
        safe_cursor = urllib.parse.quote(cursor, safe="")
        try:
            results = self._get_url_(url + safe_cursor, self.headers)
        except (ConnectionError, requests.exceptions.ConnectionError) as e:
            retries += 1
            return self._handle_connection_error(retries, max_retries, url, cursor, e)

        logging.info(f"Querying: {url + safe_cursor}")
        message = results['message']
        items = message['items']
        total_results = message['total-results']
        items_processed = 0
        for item in items:
            items_processed += 1
            logging.info(f"Processing DOI: {item['DOI']}")
            type = item['type']
            title = item['title']
            print(f"title: {title}")
            for author in item['author']:
                full_name = f"{author['given']} {author['family']}"
                print(" " + full_name)
            # Process item here.


        if len(items) == 0:
            logging.error("No items left.")
            raise ConnectionError()
        else:
            logging.info(f"Processed {len(items)} items")
        return message['next-cursor'], total_results, items_processed

    def _handle_connection_error(self, retries, max_retries, url, cursor, e):
        """    Handles connection errors during downloading.

        :param retries: The number of retries attempted.
        :type retries: int
        :param max_retries: The maximum number of retries allowed.
        :type max_retries: int
        :param url: The URL of the resource being downloaded.
        :type url: str
        :param cursor: The cursor or pointer indicating the current position in the resource.
        :type cursor: int
        :type start_year: int
        :param e: The connection error that occurred.
        :type e: Exception
        :raises RetriesExceededException: If the maximum number of retries is exceeded.
        :return: The result of the _download_chunk function call.
        :rtype: Result of _download_chunk function call.
        """
        logging.info(f"Connection error: {e}, retries: {retries}. Sleeping 60 and retrying.")
        time.sleep(60)
        if retries >= max_retries:
            raise RetriesExceededException(f"Retried {retries} times, aborting.")
        return self._download_chunk(url, cursor, retries)
