import traceback
from os.path import join
import json

import pandas as pd
from pymongo import MongoClient

from interpreters.base import baseInterpreter
from libs.utilsInitialProcessing import dropByPercentageJSON, detectAndExtractSubstrings, cleanUrl
from libs.customExceptions import NoMatch

class GoogleInterpreter(baseInterpreter):

    def __init__(self, debug=False):
        self.originalData = None
        self.data = None
        self.debug = debug

        self.actionToContentType = {
            'Searched for': 'Query',
            'Visited': 'Webpage',
        }

    def load(self, path, name):
        assert(isinstance(name, str))
        fullPath = join(path, name)
        with open(fullPath, encoding="utf-8") as f:
            data = json.load(f)

        if self.debug:
            print('Dropping 90% DF')
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
                action, textualFeature = detectAndExtractSubstrings(['Searched for', 'Visited'], row['title'])

                # Define variables accordingly
                contentType = self.actionToContentType[action]

                if contentType == 'Query':
                    # Create datapoint
                    newDataPoint = {
                        'platform': 'Google Search',
                        'timestamp': row['time'],
                        'type': contentType,
                        'query': textualFeature,
                    }
                elif contentType == 'Webpage':
                    # Remove useless part of URL inherent to the dataset
                    url = row['titleUrl'].replace('https://www.google.com/url?q=', '')
                    url = cleanUrl(url)

                    # Create datapoint
                    newDataPoint = {
                            'platform': 'Google Search',
                        'timestamp': row['time'],
                        'type': contentType,
                        'title': textualFeature,
                        'url': url,
                    }
                else:
                    raise Exception(f'Unknown content type: {contentType}')

                self.data.append(newDataPoint)

            except NoMatch:
                continue
            except Exception as ex:
                print(traceback.format_exc())



