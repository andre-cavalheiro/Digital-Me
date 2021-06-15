import traceback
import logging

from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize

import libs.pandasLib as pl
from bson.objectid import ObjectId


def getPayloadsDfFromDB(collection, entityExtractionKeys):
    '''
    :param collection: collection client
    :param entityExtractionKeys: Dictionary, platform names: array(attributes to access)
    :return:
    '''

    df = pd.DataFrame(columns=['payload', 'id'])

    for platform, dt in entityExtractionKeys.items():
        for contentType, payloadAttrs in dt.items():
            if payloadAttrs is None:
                continue

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
    df = pl.unrollListOfDictsAttr(df, 'extractedEntities', ['_id'])

    return df


def getContentDocsWithInherentTags(collection, attrPerContentType):
    outputDf = None

    for platform, dt in attrPerContentType.items():
        for contentType, attrs in dt.items():
            if attrs is None:
                continue

            for attr in attrs:
                results = collection.find({'platform': platform, 'type': contentType, attr: {'$exists': True}})
                df = pd.json_normalize(results)

                # Assuming these attributes hold arrays - unroll into several rows
                df = pl.unrollListAttr(df, attr, ['_id'], newAttrName='normalized')

                if outputDf is None:
                    outputDf = df
                else:
                    outputDf = outputDf.append(df)


    return outputDf


def getAllDocs(collection):
    results = collection.find({})
    return results


def getMinMaxDay(collection):
    results = collection.aggregate([
        {
            "$unwind": {
                           "path": '$timestamp',
                           "preserveNullAndEmptyArrays": False
           }
        },
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


def saveMany(collection, data):
    assert(data is not None)
    try:
        insertedDocs = collection.insert_many(data)
        insertedIds = insertedDocs.inserted_ids
    except Exception as ex:
        print(traceback.format_exc())
    return insertedIds


def getFromId(id, *collections):
    for c in collections:
        results = c.find({'_id': ObjectId(id)})
        results = list(results)
        if len(results) > 0:
            if len(results) > 1:
                logging.warning(f'More than 1 result when searching from ID - {id}')
            return results[0], c
    raise Exception('No ID match')