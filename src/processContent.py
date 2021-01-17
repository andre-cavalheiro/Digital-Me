import traceback
import logging
from os.path import join

from pymongo import MongoClient

from libs.mongoLib import saveMany, updateContentDocs
from libs.yamlLib import loadYaml
from libs.utilsInitialProcessing import reorganize
from interpreters.google import GoogleInterpreter
from interpreters.youtube import YoutubeInterpreter
from interpreters.twitter import TwitterInterpreter
from interpreters.reddit import RedditInterpreter
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
        },
        'locationKeys': {
            'Video': ['channel'],
            'Query': None
        },
        'locationsToIgnore': [],
        'locationLabelPerKey': {
            'channel': 'YouTube Channel',
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
        },
        'locationKeys': {
            'Comment': ['targetContentAuthor', 'targetContentFbLocation'],
            'Post': ['targetContentAuthor', 'targetContentFbLocation', 'mentions'],
            'Query': None,
        },
        'locationsToIgnore': ['Self'],
        'locationLabelPerKey': {
            'targetContentAuthor': 'Facebook Account',
            'targetContentFbLocation': 'Facebook Location',
            'mentions': 'Facebook Account',
        }
    },
    'Google Search': {
        'class': GoogleInterpreter,
        'file': 'Google/A_minha_atividade.json',
        'termsToIgnore': ["Used Search"],
        'keysForMerge': {
            'Query': ['query'],
            'Webpage': ['title'],
        },
        'locationKeys': {
            'Query': None,
            'Webpage': ['url'],
        },
        'locationsToIgnore': [],
        'locationLabelPerKey': {
            'url': 'URL',
        }
    },
    'Reddit': {
        'class': RedditInterpreter,
        'file': ['Reddit/comments.csv', 'Reddit/posts.csv'],
        'termsToIgnore': [],
        'keysForMerge': {
            'Comment': None,
            'Post': None,
        },
        'locationKeys': {
            'Comment': ['subreddit'],
            'Post': ['subreddit'],
        },
        'locationsToIgnore': [],
        'locationLabelPerKey': {
            'subreddit': 'Subreddit',
        }
    },
    'Twitter': {
        'class': TwitterInterpreter,
        'file': ['Twitter/tweet.json'],
        'termsToIgnore': [],
        'keysForMerge': {
            'Tweet': None
        },
        'locationKeys': {
            'Tweet': ['userMentions'],
        },
        'locationsToIgnore': [],
        'locationLabelPerKey': {
            'userMentions': 'Twitter Account',
        }
    },
}

interpreters = {
    'Facebook': FacebookInterpreter,
    'YouTube': YoutubeInterpreter,
    'GoogleSearch': GoogleInterpreter,
    'Reddit': RedditInterpreter,
    'Twitter': TwitterInterpreter,

}

if __name__ == '__main__':

    # Set up logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    dataDir = '../data/'
    configDir = '../configs/'
    # Load config
    platforms = loadYaml('../configs/platforms.yaml')

    # Set up DB
    client = MongoClient()
    db = client['digitalMe']
    collectionCont = db['content']
    collectionLoc = db['locations']

    for platform, configFile in platforms.items():
        try:
            # Load config file
            print(f'=== {platform} ===')
            info = loadYaml(join(configDir, configFile))

            # Content processing
            interpreter = interpreters[platform](debug=False)
            interpreter.load(dataDir, info['file'])
            interpreter.preProcess()
            interpreter.transform(info['termsToIgnore'])
            interpreter.mergeSameContent(info['keysForMerge'])
            contentData = interpreter.getContentData()
            contentDocsIds = saveMany(collectionCont, contentData)

            minDate, maxDate = interpreter.getMinMaxTime()
            print(f'History from {minDate} to {maxDate}')

            # Locations processing
            interpreter.addIds(contentDocsIds)
            interpreter.extractLocations(info['locationKeys'], info['locationsToIgnore'], info['locationLabelPerKey'], platform=platform)
            locationData = interpreter.getLocationData()
            locationDocsIds = saveMany(collectionLoc, locationData)
            contentDocsPayload = reorganize(locationData, locationDocsIds)  # Fix this name, it's stupid
            updateContentDocs(collectionCont, 'locations', contentDocsPayload)

        except Exception as ex:
            print(traceback.format_exc())
