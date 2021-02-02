import os
import sys
sys.path.append(os.path.join(os.getcwd(), '..'))   # Add src/ dir to import path
import traceback
import logging
from os.path import join

from pymongo import MongoClient

from libs.osLib import loadYaml
from libs.mongoLib import saveMany, updateContentDocs
from libs.utilsInitialProcessing import invertCollectionPriority
from interpreters.google import GoogleInterpreter
from interpreters.youtube import YoutubeInterpreter
from interpreters.twitter import TwitterInterpreter
from interpreters.reddit import RedditInterpreter
from interpreters.facebook import FacebookInterpreter

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

    # Load config
    configDir = '../../configs/'
    config = loadYaml(join(configDir, 'main.yaml'))

    # Set up DB
    client = MongoClient()
    db = client['digitalMe']
    collectionCont = db['content']
    collectionLoc = db['locations']

    for platform, configFile in config['platforms'].items():
        try:
            # Load config file
            print(f'=== {platform} ===')
            info = loadYaml(join(configDir, configFile))

            # Content processing
            interpreter = interpreters[platform](debug=False)
            interpreter.load(config['dataDir'], info['file'])
            interpreter.preProcess()
            interpreter.transform(info['termsToIgnore'])
            interpreter.mergeSameContent(info['keysForMerge'])
            contentData = interpreter.getContentData()
            contentDocsIds = saveMany(collectionCont, contentData)
            minDate, maxDate = interpreter.getMinMaxTime()
            print(f'History from {minDate} to {maxDate}')

            # Locations processing
            interpreter.addIds(contentDocsIds)
            locationData = interpreter.extractLocations(info['sourceKeys'], info['sourcesToIgnore'],
                                                        info['sourceTypePerKey'], info['sourceRelationship'],
                                                        platform=platform)
            locationDocsIds = saveMany(collectionLoc, locationData)
            contentDocsPayload = invertCollectionPriority(locationData, locationDocsIds)  # Fix this name, it's stupid
            updateContentDocs(collectionCont, 'locations', contentDocsPayload)

        except Exception as ex:
            print(traceback.format_exc())
