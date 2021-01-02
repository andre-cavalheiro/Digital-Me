import traceback
import logging
import string
import random

import pandas as pd
from pymongo import MongoClient
from rosette.api import API, DocumentParameters, RosetteException
import bisect

from libs.mongoLib import getPayloadDfFromDB, updateContentDocsWithRawEntities

platforms = {
    'Google Search': {
        'Query': ['query'],
        'Webpage': ['title'],
    },
    'Youtube': {
        'Query': ['query'],
        'Video': ['title'],
    },
    'Reddit': {
        'Comment': ['body'],
        'Post': ['title', 'body'],
    },
    'Twitter': {
        'Tweet': ['body']
    },
    'Facebook': {
        'Comment': ['body'],
        'Post': ['body'],
        'Event': ['name'],
        'Query': ['query'],
    }
}


def generateDocuments(df, maxCharsPerDoc):

    df['doc'] = None
    df['index-range'] = None

    docs, docIndx = [''], 0

    for index, row in df.iterrows():
        if pd.isnull(row['payload']):
            continue
        payload = row['payload'] + '. '  # Separate the different payloads by a dot and a space.
        size = len(payload)

        if len(docs[docIndx])+size > maxCharsPerDoc:
            docs.append('')
            docIndx += 1


        df.at[index, 'doc'] = docIndx
        df.at[index, 'index-range'] = [len(docs[docIndx]), len(docs[docIndx])+size]
        docs[docIndx] += payload

    return docs, df


def extractEntities(rosetteClient, doc):
    try:
        params = DocumentParameters()
        params['content'] = doc
        params['genre'] = 'social-media'  # Text genre

        results = rosetteClient.entities(params)
        entities = results['entities']

        if type(entities) is pd.Series:
            entities = entities.iloc[0]
        assert(type(entities) is list)

    except RosetteException as ex:
        if ex.status == 'overPlanLimit':
            logging.error('REACHED TODAY\'S MAXIMUM')
            exit()
        logging.error('> [Error] Problem in rosette')
        logging.error(traceback.format_exc())
    except Exception as ex:
        logging.error(traceback.format_exc())

    return entities


def attachEntitiesToPayload(entities, df):

    separationIndexes = [row['index-range'][1] for _, row in df.iterrows()]
    df.loc[:, 'entities'] = pd.Series([[] for _ in range(df.shape[0])])

    # Add entities to dataframe
    for entity in entities:
        entityData = entity.copy()
        del entityData['count']
        del entityData['mentionOffsets']

        for mention in entity['mentionOffsets']:
            # Identify payload associated with this mention
            offset = mention['endOffset']
            payloadIndex = bisect.bisect_left(separationIndexes, offset)
            payloadIndex = payloadIndex-1 if payloadIndex == df.shape[0] else payloadIndex   # Handle bisect_left functionality for our case use.

            # Attribute value
            df.at[payloadIndex, 'entities'] = df.loc[payloadIndex]['entities'] + [entityData]   # Append
    return df


def idGenerator(size, chars=string.digits):
    return 'F' + ''.join(random.choice(chars) for _ in range(size))


def randID(entities, idSize):
    for entity in entities:
        if not entity['entityId'].startswith('Q'):
            entity['entityId'] = idGenerator(idSize)
    return entities


if __name__ == '__main__':

    maxChars = 45000
    idSize = 6

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # DB client
    client = MongoClient()
    db = client['digitalMe']
    collection = db['content']

    logging.info(f'Loading payloads from DB')
    df = getPayloadDfFromDB(collection, platforms)
    docs, df = generateDocuments(df, maxChars)

    # Rosette client
    rosetteClient = API(user_key='83ae01e826896d296a3a8d29b841eef3')

    logging.info(f'Created {len(docs)} documents')
    for it, doc in enumerate(docs):

        docDf = df.loc[df['doc'] == it].reset_index(drop=True)

        # Extract entities
        extractedEntities = extractEntities(rosetteClient, doc)

        # Provide ID for the ones who aren't linked
        extractedEntities = randID(extractedEntities, idSize)

        docDf = attachEntitiesToPayload(extractedEntities, docDf)

        updateContentDocsWithRawEntities(collection, docDf)

        logging.info(f'Done with iteration {it} - {len(extractedEntities)} entities extracted')
