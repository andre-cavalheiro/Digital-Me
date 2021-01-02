import traceback

from pymongo import MongoClient

platforms = {
    'Facebook': {
        'Comment': 'contextAuthor',
        'Post': 'location',
        'Event': None,
        'Query': None,
    },
    'Youtube': {
        'Video': 'channel',
        'Query': None
    },
    'Google Search': {
        'Query': None,
        'Webpage': 'url',
    },
    'Reddit': {
        'Comment': 'subreddit',
        'Post': 'subreddit',
    },
    'Twitter': {
        'Tweet': None,
    },
}


if __name__ == '__main__':

    baseDir = '../data/'

    client = MongoClient()
    db = client['digitalMe']
    collectionCont = db['content']
    collectionLoc = db['locations']


    try:
        pass
    except Exception as ex:
        print(traceback.format_exc())
