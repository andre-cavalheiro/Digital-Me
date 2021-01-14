import traceback
import logging

from pymongo import MongoClient

from interpreters.google import GoogleInterpreter
from interpreters.youtube import YoutubeInterpreter
from interpreters.twitter import TwitterInterpreter
from interpreters.reddit import RedditInterpreter
from interpreters.facebook import FacebookInterpreter
from interpreters.facebook import FacebookInterpreter

# todo - transform this into config file
platforms = {
    'Youtube': {
        'class': YoutubeInterpreter,
        'file': 'YouTube/A_minha_atividade.json',
        'termsToIgnore': ["a video that has been removed"],
        'keysForMerge': {
            'Query': ['query'],
            'Video': ['channel', 'title'],
        }
    },
    'Facebook': {
        'class': FacebookInterpreter,
        'file': [
            'Facebook/comments/comments.json', 'Facebook/likes_and_reactions/posts_and_comments.json',
            'Facebook/posts/your_posts.json', 'Facebook/posts/other_people\'s_posts_to_your_timeline.json',
            'Facebook/search_history/your_search_history.json'
        ],
        'termsToIgnore': [],
        'keysForMerge': {
            'Comment': None,
            'Post': None,
            'Query': ['query'],
        }
    },
    'Google Search': {
        'class': GoogleInterpreter,
        'file': 'Google/A_minha_atividade.json',
        'termsToIgnore': ["Used Search"],
        'keysForMerge': {
            'Query': ['query'],
            'Webpage': ['title'],
        }
    },
    'Reddit': {
        'class': RedditInterpreter,
        'file': ['Reddit/comments.csv', 'Reddit/posts.csv'],
        'termsToIgnore': [],
        'keysForMerge': {
            'Comment': None,
            'Post': None,
        }
    },
    'Twitter': {
        'class': TwitterInterpreter,
        'file': ['Twitter/tweet.json'],
        'termsToIgnore': [],
        'keysForMerge': {
            'Tweet': None
        }
    },
}

"""
"""

if __name__ == '__main__':

    # Set up logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir = '../data/'

    client = MongoClient()
    db = client['digitalMe']
    collection = db['content']

    for platform, info in platforms.items():
        try:
            print(f'=== {platform} ===')
            interpreter = info['class'](debug=False)
            interpreter.load(baseDir, info['file'])
            interpreter.preProcess()
            interpreter.transform(info['termsToIgnore'])
            interpreter.mergeTheSameContent(info['keysForMerge'])
            minDate, maxDate = interpreter.getMinMaxTime()
            print(f'History from {minDate} to {maxDate}')
            interpreter.storeInDB(collection)

        except Exception as ex:
            print(traceback.format_exc())
