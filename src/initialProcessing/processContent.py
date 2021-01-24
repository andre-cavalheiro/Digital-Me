from os import getcwd
import sys
sys.path.append(getcwd() + '/..')   # Add src/ dir to import path
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

    dataDir = '../../data/'
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
            # contentDocsIds = saveMany(collectionCont, contentData)
            contentDocsIds = None
            minDate, maxDate = interpreter.getMinMaxTime()
            print(f'History from {minDate} to {maxDate}')
            exit()
            # Locations processing
            interpreter.addIds(contentDocsIds)
            interpreter.extractLocations(info['locationKeys'], info['locationsToIgnore'], info['locationLabelPerKey'], platform=platform)
            locationData = interpreter.getLocationData()
            locationDocsIds = saveMany(collectionLoc, locationData)
            contentDocsPayload = reorganize(locationData, locationDocsIds)  # Fix this name, it's stupid
            updateContentDocs(collectionCont, 'locations', contentDocsPayload)

        except Exception as ex:
            print(traceback.format_exc())
