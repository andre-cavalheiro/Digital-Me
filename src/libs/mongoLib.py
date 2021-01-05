import traceback

from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize


def getPayloadsDfFromDB(collection, platforms):
    '''
    :param collection: collection client
    :param platforms: Dictionary, platform names: array(attributes to access)
    :return:
    '''

    df = pd.DataFrame(columns=['payload', 'id'])

    for platform, dt in platforms.items():
        for contentType, payloadAttrs in dt.items():
            results = collection.find({'$and': [{'platform': platform}, {'type': contentType}]})

            # Too slow - probably should creat df from dics and then change as I see fit
            for r in results:
                for att in payloadAttrs:
                    newDataPoint = {
                        'id': r['_id'],
                        'payload': r[att],
                    }
                    df = df.append(newDataPoint, ignore_index=True)
    return df


def getContentDocsPerPlatform(collection, platforms):
    '''
    :param collection: collection client
    :param platforms: list of platforms from which content documents are desired
    :return: generator - if necessary to convert to a list just list(content). Generator better for large results
    '''
    content = collection.find({'platform': {'$in': platforms}})
    return content


def getContentDocsWithEntities(collection):
    '''
    :param collection:
    :return: pd.DataFrame
    '''
    results = collection.find({'extractedEntities': {'$exists': True, '$ne': []}})
    df = pd.json_normalize(results)
    df = df[['_id', 'extractedEntities']]

    # One row per entity extracted, keeping the document id in each
    auxdf = df['extractedEntities'].apply(pd.Series).reset_index()
    auxdf = auxdf.melt(id_vars='index')
    auxdf = auxdf.dropna()[['index', 'value']]
    auxdf = auxdf.set_index('index')
    df = pd.merge(auxdf, df[['_id']], left_index=True, right_index=True)
    df.reset_index(inplace=True, drop=True)

    # Unpack dictionary
    k = json_normalize(df['value'].tolist())
    df = df.join(k)
    df.drop('value', axis=1, inplace=True)

    return df


def getAllDocs(collection):
    results = collection.find({})
    return results


def getMinMaxDay(collection):
    results = collection.aggregate([
        {
            "$group": {
                    "_id": "minMaxDate",
                    "maxDate": {"$max": "$timestamp"},
                    "minDate": {"$min": "$timestamp"},
            }
        }
    ])
    results = list(results)[0]
    return results['minDate'].date(), results['maxDate'].date()



def updateContentDocsWithRawEntities(collection, docDf):
    '''
    :param collection: collection client
    :param docDf: pd.DataFrame
    '''
    for index, row in docDf.iterrows():
        instanceId = row['id']
        collection.update_one(
            {"_id": instanceId},
            {"$set": {"extractedEntities": row['entities']}}
        )


def updateContentDocs(collection, key, contentDocsPayload):
    '''
    :param collection: collection client
    :param entityInfo: list
    :param entityIds: list
    '''
    for k, v in contentDocsPayload.items():
        collection.update_one(
            {"_id": k},
            {"$set": {key: v}}
        )

