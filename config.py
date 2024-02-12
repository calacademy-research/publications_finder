import configparser
from os.path import exists
import json
import sys

CONFIGFILE = "./config.ini"


class Config():
    def __init__(self):
        self.config = configparser.ConfigParser()

        if not exists(CONFIGFILE):
            print("Config file missing; copy config.temptate.ini to config.ini and customize.")
            return None
        else:
            self.config.read(CONFIGFILE)

    def get_int(self, section, param):
        return self.config.getint(section, param)

    def get_string(self, section, param):
        return self.config[section][param]

    def get_boolean(self, section, param):
        return self.config.getboolean(section, param)

    def get_list(self, section, param):
        results = self.get_string(section, param)
        results = results.replace('\n', '')
        if len(results) == 0:
            return None
        else:
            return eval(results)
