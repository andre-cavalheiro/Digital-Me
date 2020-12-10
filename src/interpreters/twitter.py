import traceback
from os.path import join
import json

import pandas as pd
from pymongo import MongoClient

from interpreters.base import baseInterpreter
from libs.actionPayloadUtils import dropByPercentageJSON


class TwitterInterpreter(baseInterpreter):

    def __init__(self, debug=False):
        self.originalData = None
        self.data = None
        self.debug = debug

    def load(self, path, files):
        assert (isinstance(files, list))
        self.originalData = {}

        for fileName in files:
            fullPath = join(path, fileName)
            with open(fullPath, encoding="utf-8") as f:
                data = json.load(f)

            if self.debug:
                print('Dropping 90% DF')
                data, _ = dropByPercentageJSON(data, 0.9)

            dfName = fileName.split('Twitter/')[1].split('.json')[0]
            self.originalData[dfName] = data

    def preProcess(self):
        assert(self.originalData is not None)
        assert('tweet' in self.originalData.keys())

        tweetsData = []
        for row in self.originalData['tweet']:
            try:
                dataPoint = row['tweet']

                dataPoint['created_at'] = pd.to_datetime(dataPoint['created_at'])

                # Drop useless stuff
                del dataPoint['retweeted']
                del dataPoint['favorited']
                del dataPoint['source']
                del dataPoint['truncated']
                del dataPoint['id_str']
                del dataPoint['id']
                del dataPoint['display_text_range']

                if 'possibly_sensitive' in dataPoint.keys():
                    del dataPoint['possibly_sensitive']
                if 'in_reply_to_status_id_str' in dataPoint.keys():
                    del dataPoint['in_reply_to_status_id_str']
                if 'in_reply_to_user_id_str' in dataPoint.keys():
                    del dataPoint['in_reply_to_user_id_str']

                # Change to proper data types
                dataPoint['retweet_count'] = int(dataPoint['retweet_count'])
                dataPoint['favorite_count'] = int(dataPoint['favorite_count'])

                tweetsData.append(dataPoint)
            except Exception as ex:
                print(traceback.format_exc())

        self.originalData['tweet'] = tweetsData

    def transform(self, termsToIgnore):
        assert('tweet' in self.originalData.keys())

        self.data = []
        for row in self.originalData['tweet']:
            try:
                newDataPoint = {
                    'platform': 'Twitter',
                    'timestamp': row['created_at'],
                    'type': 'Tweet',
                    'body': row['full_text'],
                    'likes': row['favorite_count'],
                    'retweets': row['retweet_count'],
                    'language': row['lang'],
                    'mentions': row['entities'],
                }
                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())