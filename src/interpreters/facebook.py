import traceback
from os.path import join
import json

import pandas as pd
from pymongo import MongoClient
from libs.actionPayloadUtils import dropByPercentageJSON, extractContextFromFBComment, extractContextFromFBPosts
from libs.customExceptions import Ignore

from interpreters.base import baseInterpreter

class FacebookInterpreter(baseInterpreter):

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

            dfName = fileName.split('Facebook/')[1].split('.json')[0]
            self.originalData[dfName] = data

    def preProcess(self):
        assert(self.originalData is not None)
        assert('comments' in self.originalData.keys())
        assert('your_event_responses' in self.originalData.keys())
        assert('your_posts' in self.originalData.keys())
        assert('comments' in self.originalData.keys())
        assert('your_search_history' in self.originalData.keys())

        self.originalData['comments'] = self._processComments()
        self.originalData['your_posts'] = self._processPosts()
        self.originalData['your_search_history'] = self._processSearches()

    def transform(self, termsToIgnore):
        self.data = []

        self._addPosts()
        self._addSearches()
        self._addComments()
        self._addEvents()
        # self._addCommentsPostsInGroups()

    # ----
    def _processComments(self):
        data = []
        for row in self.originalData['comments']:
            try:
                dataPoint = row['data'][0]['comment']

                if 'comment' not in dataPoint.keys():
                    dataPoint['comment'] = ''   # Comments without any text (just attachments)

                if 'attachments' in row.keys():
                    assert(len(row['attachments']) == 1)
                    dataPoint['attachments'] = [attach['data'] for attach in row['attachments']]
                else:
                    dataPoint['attachments'] = []

                dataPoint['timestamp'] = pd.to_datetime(dataPoint['timestamp'])

                # Process text to extract comment's context
                type, author = extractContextFromFBComment(row['title'])
                # print(f'{type} - {author}')
                dataPoint['contextAuthor'] = author
                dataPoint['contextType'] = type

                data.append(dataPoint)
            except Exception as ex:
                print(traceback.format_exc())
        return data

    def _addComments(self):
        for row in self.originalData['comments']:
            try:
                newDataPoint = {
                    'platform': 'Facebook',
                    'timestamp': row['timestamp'],
                    'type': 'Comment',
                    'body': row['comment'],
                    'attachments': row['attachments'],
                    'contextAuthor': row['contextAuthor'],
                    'contextType': row['contextType'],
                }
                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())

    # ----
    def _processPosts(self):
        data = []
        for row in self.originalData['your_posts']:
            try:
                if 'data' in row.keys():
                    row['post'] = row['data'][0]['post'] if 'post' in row['data'][0].keys() else ''
                    del row['data']
                else:
                    continue

                row['timestamp'] = pd.to_datetime(row['timestamp'])

                if 'attachments' in row.keys():
                    row['attachments'] = [attach['data'] for attach in row['attachments']]
                else:
                    row['attachments'] = []

                if 'tags' not in row.keys():
                    row['tags'] = []

                if 'title' in row.keys():
                    action, location = extractContextFromFBPosts(row['title'])

                    row['action'] = action
                    row['location'] = location
                    '''
                    print(row['title'])
                    print(f'{action}  {location}')
                    print('-------')
                    '''

                else:
                    row['action'] = None
                    row['location'] = None
                    ''''
                    print('=========')
                    print(row['post'])
                    print('=========')
                    '''

                data.append(row)
            except Ignore:
                pass
            except Exception as ex:
                print(traceback.format_exc())
        return data

    def _addPosts(self):
        for row in self.originalData['your_posts']:
            try:
                newDataPoint = {
                    'platform': 'Facebook',
                    'timestamp': row['timestamp'],
                    'type': 'Post',
                    'action': row['action'],
                    'location': row['location'],
                }
                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())

    # ----

    def _processEvents(self):
        for row in self.originalData['your_event_responses']:
            try:
                row['timestamp'] = pd.to_datetime(row['timestamp'])
            except Exception as ex:
                print(traceback.format_exc())

    def _addEvents(self):
        for row in self.originalData['your_event_responses']:
            try:
                newDataPoint = {
                    'platform': 'Facebook',
                    'timestamp': row['start_timestamp'],
                    'type': 'Event',
                    'location': row['place'] if 'place' in row.keys() else None,
                    'endTimestamp': row['end_timestamp'],
                }
                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())

    # ----

    def _processSearches(self):
        data = []
        for row in self.originalData['your_search_history']:
            try:
                if 'data' not in row.keys():
                    if 'title' not in row.keys():
                        continue
                    else:
                        payload = row['title'].replace('You searched for ', '')
                else:
                     payload = row['data'][0]['text']
                row['data'] = payload
                row['timestamp'] = pd.to_datetime(row['timestamp'])

            except Exception as ex:
                print(traceback.format_exc())
        return data

    def _addSearches(self):
        for row in self.originalData['your_search_history']:
            try:
                newDataPoint = {
                    'platform': 'Facebook',
                    'timestamp': row['timestamp'],
                    'type': 'Query',
                    'query': row['data'],
                }
                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())

    """
    def _processCommentsPostsInGroups(self):
        data = []
        for row in self.originalData['your_posts_and_comments_in_groups']:
            try:
                dataPoint = row['data'][0]['comment']
                if 'comment' not in dataPoint.keys():
                    dataPoint['comment'] = ''
                data.append(dataPoint)
            except Exception as ex:
                print(traceback.format_exc())
        return data
    def _addCommentsPostsInGroups(self):
        for row in self.originalData['your_posts_and_comments_in_groups']:
            try:
                # todo
                newDataPoint = {
                    'platform': 'Facebook',
                    'timestamp': row['timestamp'],
                    'type': '',
                }
                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())
    """