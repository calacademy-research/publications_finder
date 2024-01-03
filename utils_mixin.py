from json import JSONDecodeError

import requests
import ntpath
import time
import re
import logging

class Utils:
    def __init__(self):
        self.response_time = 0

    def _get_url_(self, url, headers=None, decode_json=True):
        """Perform an HTTP GET request to "https://api.crossref.org/works/{doi_string}"
            and return JSON response, decoded

        :param url: The URL to send the GET request to.
        :type url: str

        :param headers: Optional headers to include in the request, defaults to None.
        :type headers: dict, optional

        :param decode_json: Flag indicating whether to decode the 
            response as JSON, defaults to True.
        :type decode_json: bool, optional

        :raises ConnectionError: If request to url fails. Provides the url.
        :raises ConnectionError: If there is an error decoding the JSON response.
            Provides the string representation of the caught exceptions.
        
        :return: The response from the URL, either as a decoded JSON object (if `decode_json` is True) or as a raw response.
        :rtype: dict or requests.Response
        """        
        if self.response_time > 20:
            logging.info(f"Long wait time ({self.response_time} seconds), backing off 60 seconds on request {url}")
            time.sleep(60)
        elif self.response_time > 10:
            logging.info(f"Long wait time ({self.response_time} seconds), backing off 30 seconds on request {url}")
            time.sleep(30)
        elif self.response_time > 4:
            logging.info(f"Long wait time ({self.response_time} seconds), backing off 5 seconds on request {url}")
            time.sleep(5)

        start = time.time()

        if headers is None:
            response = requests.get(url, allow_redirects=True)
        else:
            response = requests.get(url, allow_redirects=True, headers=headers)
        self.response_time = time.time() - start
        logging.info(f"Request took {self.response_time}")
        if response.status_code != 200:
            # logging.error(f"Fail to get url: {url} ")
            raise ConnectionError(url)
        if decode_json:
            try:
                return response.json()
            except JSONDecodeError as e:
                logging.error(f"Invalid JSON:\n{response}")
                raise ConnectionError(f"{e}")
        else:
            return response



