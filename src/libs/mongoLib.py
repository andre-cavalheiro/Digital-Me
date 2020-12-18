import traceback

from pymongo import MongoClient
import pandas as pd

def getPayloadDfFromDB(client, platforms):
    '''
    :param client: collection client
    :param platforms: Dictionary, platform names: array(attributes to access)
    :return:
    '''

    df = pd.DataFrame(columns=['payload', 'id'])

    for platform, dt in platforms.items():
        for contentType, payloadAttrs in dt.items():
            results = client.find({'$and': [{'platform': platform}, {'type': contentType}]})
            for r in results:
                for att in payloadAttrs:
                    newDataPoint = {
                        'id': r['_id'],
                        'payload': r[att],
                    }
                    df = df.append(newDataPoint, ignore_index=True)
    return df

def sendEntitiesToDB(collection, docDf):

    for index, row in docDf.iterrows():
        instanceId = row['id']
        collection.update_one(
            {"_id": instanceId},
            {"$set": {"extractedEntities": row['entities']}}
        )