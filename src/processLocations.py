import traceback
import logging

from pymongo import MongoClient

from libs.mongoLib import getContentDocsPerPlatform, updateContentDocs
from libs.customExceptions import ContentWithMultipleLocations

locationKeyPerContentType = {
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


def filterContent(contentList, locationKeyPerContentType):
    '''
    :param contentList:
    :param locationKeyPerContentType:
    :return: dict locationLabel: { }
    '''

    output = {}
    for content in contentList:
        try:
            platform = content['platform']
            contentType = content['type']
            locationKey = locationKeyPerContentType[platform][contentType]

            if locationKey is None:
                continue    # Content type does not have a location

            location = content[locationKey]

            if location not in output.keys():
                output[location] = {
                    'platform': platform,
                    'contentTypes': [contentType],
                    'associatedContent': [content['_id']],
                }
            else:
                output[location]['associatedContent'].append(content['_id'])

                if platform != output[location]['platform']:
                    if 'alternativePlatforms' not in output[location].keys():
                        output[location]['alternativePlatforms'].append(location)
                    else:
                        output[location]['alternativePlatforms'] = [location]
                    logging.warning(f'Location with label: \"{location}\" associated with multiple platforms')

                if contentType not in output[location]['contentTypes']:
                    output[location]['contentTypes'].append(contentType)
        except Exception as ex:
            print(traceback.format_exc())
            continue
    return output


if __name__ == '__main__':

    baseDir = '../data/'

    try:
        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionLoc = db['locations']

        contentList = getContentDocsPerPlatform(collectionCont, list(locationKeyPerContentType.keys()))
        locationDict = filterContent(contentList, locationKeyPerContentType)

        locationCollectionData = [dict(dt, label=k) for k, dt in locationDict.items()]

        # Save it to DB
        insertedDocs = collectionLoc.insert_many(locationCollectionData)
        insertedIds = insertedDocs.inserted_ids

        # Update content docs with locations
        contentDocsPayload = {}
        for info, entityId in zip(locationCollectionData, insertedIds):
            for contentDoc in info['associatedContent']:
                try:
                    if contentDoc not in contentDocsPayload.keys():
                        contentDocsPayload[contentDoc] = entityId
                    else:
                        raise ContentWithMultipleLocations
                except Exception as ex:
                    print(traceback.format_exc())

        updateContentDocs(collectionCont, 'origin', contentDocsPayload)

    except Exception as ex:
        print(traceback.format_exc())
