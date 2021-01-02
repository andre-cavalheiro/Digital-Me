import traceback
from os.path import join
import logging

import pandas as pd
from pymongo import MongoClient
import json

from interpreters.base import baseInterpreter
from libs.utilsInitialProcessing import dropByPercentageJSON, detectAndExtractSubstrings, fromDictToDf
from libs.customExceptions import NoYtChannel

class YoutubeInterpreter(baseInterpreter):

    def __init__(self, debug=False):
        self.originalData = None
        self.data = None
        self.debug = debug
        self.actionToContentType = {
            'Searched for': 'Query',
            'Watched': 'Video',
        }

    def load(self, path, name):
        assert(isinstance(name, str))
        fullPath = join(path, name)
        with open(fullPath, encoding="utf-8") as f:
            data = json.load(f)

        if self.debug:
            logging.warning('Dropping 90% DF')
            data, _ = dropByPercentageJSON(data, 0.9)

        self.originalData = data

    def preProcess(self):
        assert(self.originalData is not None)
        for row in self.originalData:
            row['time'] = pd.to_datetime(row['time'])

    def transform(self, termsToIgnore):
        self.data = []
        for row in self.originalData:
            try:
                # Check if item is valid
                if len([term for term in termsToIgnore if term in row['title']]) > 0:
                    continue

                # Extract action from payload
                action, textualFeature = detectAndExtractSubstrings(["Searched for", "Watched"], row['title'])

                # Define variables accordingly
                contentType = self.actionToContentType[action]

                if contentType == 'Video':
                    # Extract youtube channel
                    if 'subtitles' in row.keys() and isinstance(row['subtitles'], (list, tuple)):
                        channel = row['subtitles'][0]['name']
                    else:
                        raise NoYtChannel

                    # Create datapoint
                    newDataPoint = {
                        'platform': 'YouTube',
                        'timestamp': row['time'],
                        'type': contentType,
                        'title': textualFeature,
                        'url': row['titleUrl'],
                        'channel': channel,
                    }
                elif contentType == 'Query':
                    newDataPoint = {
                        'platform': 'YouTube',
                        'timestamp': row['time'],
                        'type': contentType,
                        'query': textualFeature,
                        'url': row['titleUrl'],
                    }
                else:
                    raise Exception(f'Unknown content type: {contentType}')

                self.data.append(newDataPoint)

            except NoYtChannel:
                pass
            except Exception as ex:
                logging.error(traceback.format_exc())