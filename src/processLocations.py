import traceback
import logging

from pymongo import MongoClient

from libs.mongoLib import getContentDocsPerPlatform, updateContentDocs
from libs.customExceptions import ContentWithMultipleLocations, ContentWithoutLocation

locationKeyPerContentType = {
    'Facebook': {
        'Comment': ['targetContentAuthor', 'targetContentFbLocation'],
        'Post': ['targetContentAuthor', 'targetContentFbLocation', 'mentions'],
        'Query': None,
    },
    'Youtube': {
        'Video': ['channel'],
        'Query': None
    },
    'Google Search': {
        'Query': None,
        'Webpage': ['url'],
    },
    'Reddit': {
        'Comment': ['subreddit'],
        'Post': ['subreddit'],
    },
    'Twitter': {
        'Tweet': ['userMentions'],
    },
}

locationsToIgnore = {
    'Facebook': ['Self'],
    'Youtube': [],
    'Google Search': [],
    'Reddit': [],
    'Twitter': [],
}


def filterContent(contentList, locationKeyPerContentType, locationsToIgnorePerPlatform):
    '''
    :param contentList:
    :param locationKeyPerContentType:
    :return: dict locationLabel: { }
    '''

    output = {}
    err = []

    for content in contentList:
        try:
            platform = content['platform']
            contentType = content['type']
            locationKeys = locationKeyPerContentType[platform][contentType]

            if locationKeys is None:
                # Content type does not have a location
                continue

            for locationKey in locationKeys:

                if locationKey not in content.keys():
                    raise ContentWithoutLocation

                locations = content[locationKey]

                # Generalize to both lists of locations or single locations
                locations = [locations] if not isinstance(locations, list) else locations

                for location in locations:

                    if locationKey in locationsToIgnorePerPlatform[platform]:
                        continue

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

        except ContentWithoutLocation:
            err.append(content['_id'])
        except Exception as ex:
            print(traceback.format_exc())
            continue

    if len(err) > 0:
        logging.warning(f'Found {len(err)} documents without location')

    return output


if __name__ == '__main__':

    # Set up logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir = '../data/'

    try:
        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionLoc = db['locations']

        contentList = getContentDocsPerPlatform(collectionCont, list(locationKeyPerContentType.keys()))
        locationDict = filterContent(contentList, locationKeyPerContentType, locationsToIgnore)

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
                        contentDocsPayload[contentDoc] = [entityId]
                    else:
                        contentDocsPayload[contentDoc].append(entityId)
                except Exception as ex:
                    print(traceback.format_exc())

        updateContentDocs(collectionCont, 'locations', contentDocsPayload)

    except Exception as ex:
        print(traceback.format_exc())
