import traceback
from os.path import join
import json
from datetime import datetime as dt

import pandas as pd
from pymongo import MongoClient
import libs.utilsInitialProcessing as ut
from libs.customExceptions import Ignore, NothingToDo, UnexpectedFBPost, UnexpectedFBReaction

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
                data = ut.parseObj(data)

            if self.debug:
                print('Dropping 90% DF')
                data, _ = ut.dropByPercentageJSON(data, 0.9)

            dfName = fileName.split('Facebook/')[1].split('.json')[0]
            self.originalData[dfName] = data

    def preProcess(self):
        assert(self.originalData is not None)
        assert('comments/comments' in self.originalData.keys())
        assert('likes_and_reactions/posts_and_comments' in self.originalData.keys())
        assert('posts/your_posts' in self.originalData.keys())
        assert('posts/other_people\'s_posts_to_your_timeline' in self.originalData.keys())
        assert('search_history/your_search_history' in self.originalData.keys())
        # assert('events/your_event_responses' in self.originalData.keys())

        self.originalData['comments/comments'] = self._processComments()

        self.originalData['likes_and_reactions/posts_and_comments'] = self._processReactions()
        self.originalData['posts/other_people\'s_posts_to_your_timeline'] = self._processPostsFromOtherPeople()
        self.originalData['posts/your_posts'] = self._processPosts()
        self.originalData['search_history/your_search_history'] = self._processSearches()
        # self.originalData['your_event_responses'] = self._processEvents()

    def transform(self, termsToIgnore):
        self.data = []

        self._addPosts()
        self._addSearches()
        self._addComments()
        # self._addEvents()
        # self._addCommentsPostsInGroups()

    # ----
    def _processComments(self):
        data = []
        for row in self.originalData['comments/comments']['comments']:
            try:
                # Extract relevant data from array form
                dataPoint = row['data'][0]['comment']

                if dataPoint['timestamp'] == 0:
                    # Ignore faulty timestamps - nothing we can do here
                    continue

                if dt.fromtimestamp(dataPoint['timestamp']).year > 2020 or dt.fromtimestamp(dataPoint['timestamp']).year < 2010:
                    breakpoint()

                dataPoint['timestamp'] = dt.fromtimestamp(dataPoint['timestamp'])

                dataPoint['comment'] = '' if 'comment' not in dataPoint.keys() else dataPoint['comment']    # Some comments without any text (just attachments)

                dataPoint['attachments'] = [attach['data'] for attach in row['attachments']] if 'attachments' in row.keys() else []

                if 'group' in dataPoint.keys():
                    dataPoint['targetContentFbLocation'] = dataPoint['group']

               # Process text to extract comment's context
                type, action, author = ut.extractContextFromFBComment(row['title'])

                dataPoint['userAction'] = action    # It's always the same here, but usefull to add for standardization
                dataPoint['targetContentType'] = type
                dataPoint['targetContentAuthor'] = author

                data.append(dataPoint)
            except Exception as ex:
                print(traceback.format_exc())

        return data

    def _addComments(self):
        for row in self.originalData['comments/comments']:
            try:
                newDataPoint = {
                    'platform': 'Facebook',
                    'timestamp': row['timestamp'],
                    'type': 'Comment',
                    'body': row['comment'],
                    'attachments': row['attachments'],

                    'userAction': row['userAction'],
                    'targetContentType': row['targetContentType'],
                    'targetContentAuthor': row['targetContentAuthor'],
                }

                if 'targetContentFbLocation' in row.keys():
                    newDataPoint['targetContentFbLocation'] = row['targetContentFbLocation']

                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())

    # ----
    def _processPosts(self):
        data = []
        for row in self.originalData['posts/your_posts']['status_updates']:
            try:
                # Extract relevant data from array form
                if 'data' in row.keys():
                    row['post'] = row['data'][0]['post'] if 'post' in row['data'][0].keys() else ''
                    # Only position 0 matters, the others are just regarding updates (edits)
                    del row['data']
                else:
                    continue

                if row['timestamp'] == 0:
                    # Ignore faulty timestamps - nothing we can do here
                    continue

                row['timestamp'] = dt.fromtimestamp(row['timestamp'])

                row['attachments'] = [attach['data'] for attach in row['attachments']] if 'attachments' in row.keys() else []

                row['mentions'] = row['tags'] if 'tags' in row.keys() else []

                if 'title' in row.keys():
                    actionPerformedByUser, targetContentType, targetContentFbLocation, extraMentions = ut.extractContextFromFbSelfPosts(row['title'])
                    row['mentions'] += extraMentions   # Currently the only use for this is to add another user's name when we post smtg on their timeline
                else:
                    # Assuming the following from what I inspected
                    actionPerformedByUser, targetContentType, targetContentFbLocation = 'Posted', 'Post', 'Self Timeline'

                row['userAction'] = actionPerformedByUser
                row['targetContentType'] = targetContentType
                row['targetContentAuthor'] = 'Self'
                row['targetContentFbLocation'] = targetContentFbLocation
                data.append(row)

            except NothingToDo:
                data.append(row)
            except Ignore:
                pass
            except Exception as ex:
                print(traceback.format_exc())
        return data

    def _processPostsFromOtherPeople(self):
        data = []
        for row in self.originalData['posts/other_people\'s_posts_to_your_timeline']['wall_posts_sent_to_you']:
            try:
                # Extract relevant data from array form
                if 'data' in row.keys():
                    row['post'] = row['data'][0]['post'] if 'post' in row['data'][0].keys() else ''
                    # Only position 0 matters, the others are just regarding updates (edits)
                    del row['data']
                else:
                    continue

                if row['timestamp'] == 0:
                    # Ignore faulty timestamps - nothing we can do here
                    continue

                row['timestamp'] = dt.fromtimestamp(row['timestamp'])

                row['attachments'] = [attach['data'] for attach in row['attachments']] if 'attachments' in row.keys() else []

                row['mentions'] = row['tags'] if 'tags' in row.keys() else []

                if 'title' in row.keys():
                    actionByFriend, targetContentType, targetContentFbLocation, targetContentAuthor = \
                        ut.extractContextFromFbOthersPosts(row['title'])
                else:
                    raise UnexpectedFBPost

                row['actionByFriend'] = actionByFriend
                row['targetContentType'] = targetContentType
                row['targetContentAuthor'] = targetContentAuthor
                row['targetContentFbLocation'] = targetContentFbLocation
                data.append(row)

            except Ignore:
                pass
            except Exception as ex:
                print(traceback.format_exc())
        return data

    def _addPosts(self):
        for row in self.originalData['posts/your_posts'] + self.originalData['posts/other_people\'s_posts_to_your_timeline']:
            try:
                if len(row['attachments']) == 0 and len(row['mentions']) == 0 and row['post'] == '':
                    continue

                newDataPoint = {
                    'platform': 'Facebook',
                    'timestamp': row['timestamp'],
                    'type': 'Post',
                    'body': row['post'],
                    'attachments': row['attachments'],
                    'mentions': row['mentions'],

                    'targetContentType': row['targetContentType'],
                    'targetContentAuthor': row['targetContentAuthor'],
                    'targetContentFbLocation': row['targetContentFbLocation']
                }

                if 'actionByFriend' in row.keys():
                    row['actionByFriend'] = row['actionByFriend']

                if 'userAction' in row.keys():
                    row['userAction'] = row['userAction']

                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())

    # ----
    def _processReactions(self):
        data = []
        for row in self.originalData['likes_and_reactions/posts_and_comments']['reactions']:
            try:
                # Extract relevant data from array form
                if 'data' in row.keys() and 'reaction' in row['data'][0].keys():
                    row['post'] = row['data'][0]['reaction']
                    # Only position 0 matters, the others are just regarding updates (edits)
                    del row['data']
                else:
                    continue

                if row['timestamp'] == 0:
                    # Ignore faulty timestamps - nothing we can do here
                    continue

                row['timestamp'] = dt.fromtimestamp(row['timestamp'])

                if 'title' in row.keys():
                    targetContentType, targetContentAuthor, targetContentLocation = ut.extractContextFromReactions(row['title'])
                else:
                    raise UnexpectedFBReaction

                row['targetContentType'] = targetContentType
                row['targetContentAuthor'] = targetContentAuthor

                if targetContentLocation is not None:
                    row['targetContentFbLocation'] = targetContentLocation

                data.append(row)
            except Ignore:
                pass
            except Exception as ex:
                print(traceback.format_exc())
        return data

    def _AddReactions(self):
        for row in self.originalData['likes_and_reactions/posts_and_comments']:
            try:
                if len(row['attachments']) == 0 and len(row['tags']) == 0 and row['post'] == '':
                    continue

                newDataPoint = {
                    'platform': 'Facebook',
                    'timestamp': row['timestamp'],
                    'type': 'Post',
                    'reaction': row['reaction'],
                    'targetContentType': row['targetContentType'],
                    'targetContentAuthor': row['targetContentAuthor'],
                }

                if 'targetContentFbLocation' in row.keys():
                    newDataPoint['targetContentFbLocation'] = row['targetContentFbLocation']

                self.data.append(newDataPoint)

            except Exception as ex:
                print(traceback.format_exc())

    # ----
    def _processSearches(self):
        data = []
        for row in self.originalData['search_history/your_search_history']['searches']:
            try:
                if 'data' not in row.keys():
                    if 'title' not in row.keys():
                        continue
                    else:
                        payload = row['title'].replace('You searched for ', '')
                else:
                     payload = row['data'][0]['text']

                row['data'] = payload

                if row['timestamp'] == 0:
                    # Ignore faulty timestamps - nothing we can do here
                    continue

                row['timestamp'] = dt.fromtimestamp(row['timestamp'])
                r=1

            except Exception as ex:
                print(traceback.format_exc())
        return data

    def _addSearches(self):
        for row in self.originalData['search_history/your_search_history']:
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
    # ----
    def _processEvents(self):
        data = []
        for row in self.originalData['your_event_responses']:
            try:
                if dt.fromtimestamp(row['start_timestamp']).year > 2020 or dt.fromtimestamp(row['start_timestamp']).year < 2010:
                    breakpoint()
                row['timestamp'] = dt.fromtimestamp(row['timestamp'])

                row['start_timestamp'] = dt.fromtimestamp(row['start_timestamp'])
                row['name'] = row['name']

                if 'place' in row.keys():
                    row['place'] = row['place']['name']
                else:
                    row['place'] = None

                if 'end_timestamp' in row.keys() and row['end_timestamp'] != 0:
                    row['end_timestamp'] = dt.fromtimestamp(row['end_timestamp'])
                else:
                    row['end_timestamp'] = None
                data.append(row)
            except Exception as ex:
                print(traceback.format_exc())
        return data

    def _addEvents(self):
        for row in self.originalData['your_event_responses']:
            try:
                newDataPoint = {
                    'platform': 'Facebook',
                    'timestamp': row['start_timestamp'],
                    'type': 'Event',
                    'name': row['name'],
                    'location': row['place'],
                    'endTimestamp': row['end_timestamp'],
                }
                self.data.append(newDataPoint)
            except Exception as ex:
                print(traceback.format_exc())

    
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