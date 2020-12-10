import traceback

from pymongo import MongoClient

from interpreters.google import GoogleInterpreter
from interpreters.youtube import YoutubeInterpreter
from interpreters.twitter import TwitterInterpreter
from interpreters.reddit import RedditInterpreter
from interpreters.facebook import FacebookInterpreter

if __name__ == '__main__':

    baseDir = '../data/'

    platforms = {
        'facebook': {
            'class': FacebookInterpreter,
            'file': ['Facebook/your_event_responses.json', 'Facebook/your_posts.json',
                     'Facebook/your_search_history.json', 'Facebook/comments.json'],
            'termsToIgnore': [],
        },
        'youtube': {
            'class': YoutubeInterpreter,
            'file': 'YouTube/A_minha_atividade.json',
            'termsToIgnore': ["a video that has been removed"],
        },
        'googleSearch': {
            'class': GoogleInterpreter,
            'file': 'Google/A_minha_atividade.json',
            'termsToIgnore': ["Used Search"],
        },
        'reddit': {
            'class': RedditInterpreter,
            'file': ['Reddit/comments.csv', 'Reddit/posts.csv'],
            'termsToIgnore': [],
        },
        'twitter': {
            'class': TwitterInterpreter,
            'file': ['Twitter/tweet.json'],
            'termsToIgnore': [],
        },
    }

    """

    """

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
            interpreter.storeInDB(collection)

        except Exception as ex:
            print(traceback.format_exc())
