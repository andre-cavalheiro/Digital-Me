import traceback
from os.path import join

import pandas as pd
from pymongo import MongoClient

from interpreters.base import baseInterpreter
from libs.actionPayloadUtils import dropByPercentageDF

class RedditInterpreter(baseInterpreter):

    def __init__(self, debug=False):
        self.originalData = None
        self.data = None
        self.debug = debug

    def load(self, path, files):
        assert (isinstance(files, list))
        self.originalData = {}

        for fileName in files:
            fullPath = join(path, fileName)
            df = pd.read_csv(fullPath, parse_dates=['date'])

            if self.debug:
                print('Dropping 90% DF')
                df, _ = dropByPercentageDF(df, 0.9)

            dfName = fileName.split('Reddit/')[1].split('.csv')[0]
            self.originalData[dfName] = df

    def transform(self, termsToIgnore):
        assert('comments' in self.originalData.keys() and 'posts' in self.originalData.keys())
        self.data = []
        for index, row in self.originalData['comments'].iterrows():
            try:
                newDataPoint = {
                    'platform': 'Reddit',
                    'timestamp': row['date'],
                    'type': 'Comment',
                    'body': row['body'],
                    'subreddit': row['subreddit'],
                    'url': row['permalink'],
                }
                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())

        for index, row in self.originalData['posts'].iterrows():
            try:
                newDataPoint = {
                    'platform': 'Reddit',
                    'timestamp': row['date'],
                    'type': 'Post',
                    'title': row['title'],
                    'body': row['body'],
                    'subreddit': row['subreddit'],
                    'url': row['permalink'],
                }
                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())

