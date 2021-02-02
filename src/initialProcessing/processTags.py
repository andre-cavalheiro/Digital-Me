from os import getcwd
import sys
sys.path.append(getcwd() + '/..')   # Add src/ dir to import path
import traceback
import logging
from os.path import join

from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
from bson.objectid import ObjectId

from libs.utilsInitialProcessing import invertCollectionPriority
from entityExtraction import idGenerator
from libs.mongoLib import updateContentDocs, getContentDocsWithEntities, getContentDocsWithInherentTags
from libs.osLib import loadYaml


def mergeEntitiesAndInherent(entityDf, inherentTagDf, idSize):
    inherentTagDf['mention'] = inherentTagDf['normalized']
    inherentTagDf['relationshipType'] = 'InherentMention'
    inherentTagDf['entityId'] = pd.Series([idGenerator(idSize) for _ in range(inherentTagDf.shape[0])])
    entityDf = entityDf.append(inherentTagDf, ignore_index=True)
    return entityDf


def standardizeIds(df):

    linkedDf = df[df.entityId.str.startswith('Q')]
    uniqueQIDs = linkedDf.entityId.unique()
    normalizedTags = df.normalized.unique()

    df['droppedQIDs'] = None

    for tag in normalizedTags:
        specificDf = df.loc[df.normalized == tag]
        conflictingIds = specificDf.entityId.unique()

        # If there's only one ID associated with that tag, it's all good.
        if len(conflictingIds) == 1:
            continue
        else:
            QIDs = [id for id in conflictingIds if id in uniqueQIDs]

            if len(QIDs) == 0:
                # If there's no QIDs among the conflicting ones, choose any of the forged IDs and standardize that one.
                df.loc[df.normalized == tag, 'entityId'] = conflictingIds[0]

            elif len(QIDs) == 1:
                # If there's exactly one QID among the conflicting ones, that one substitutes the remaining.
                df.loc[df.normalized == tag, 'entityId'] = QIDs[0]

            else:
                # If there are conflicting QIDs, choose the first one by default but keep the other ones for possible ambiguity settlement.
                df.loc[df.normalized == tag, 'entityId'] = QIDs[0]
                for index, row in df.loc[df.normalized == tag].iterrows():
                    df.at[index, 'droppedQIDs'] = QIDs
    return df


def prepareEntityCollection(df):
    output = []
    uniqueIds = df.index.unique()
    for qid in uniqueIds:
        specificDf = df.loc[qid].to_frame().transpose() if isinstance(df.loc[qid], pd.Series) else df.loc[qid]

        types = specificDf.type.unique().tolist()
        mentionForms = specificDf.mention.unique().tolist()
        normalizedForms = specificDf.normalized.unique().tolist()

        x = specificDf[['_id', 'relationshipType']].reset_index(drop=True).rename(columns={'_id': 'id'}).T
        x = x.to_dict()
        x = x.values()
        associatedContent = list(x)

        newDataPoint = {
            'id': qid,
            'types': types,
            'mentionForms': mentionForms,
            'normalizedForms': normalizedForms,
            'associatedContent': associatedContent,
        }

        if specificDf['droppedQIDs'].notna().any():
            newDataPoint['droppedQIDs'] = specificDf['droppedQIDs'][0]

        output.append(newDataPoint)
    return output


if __name__ == '__main__':

    # Set up logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    idSize = 6

    try:
        # Load config
        configDir = '../../configs/'
        config = loadYaml(join(configDir, 'main.yaml'))
        inherentTags = {p: loadYaml(join(configDir, configFile))['inherentTags'] for p, configFile in
                                config['platforms'].items()}
        # Set up DB
        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionEnt = db['entities']

        # Query DB for content with extracted entities
        entityDf = getContentDocsWithEntities(collectionCont)
        entityDf['relationshipType'] = 'ImpliedMention'

        # Query DB for tags inherent to data (not extracted entities)
        inherentTagDf = getContentDocsWithInherentTags(collectionCont, inherentTags)

        # Ensure entities with equal names have the same QID
        entityDf = mergeEntitiesAndInherent(entityDf, inherentTagDf, idSize)
        entityDf = standardizeIds(entityDf)

        # Prepare data for DB
        entityDf.set_index('entityId', inplace=True)
        entityCollectionData = prepareEntityCollection(entityDf)

        # Save it to DB
        insertedDocs = collectionEnt.insert_many(entityCollectionData)
        insertedIds = insertedDocs.inserted_ids

        # Update content docs with tags
        contentDocsPayload = invertCollectionPriority(entityCollectionData, insertedIds)
        updateContentDocs(collectionCont, 'tags', contentDocsPayload)

    except Exception as ex:
        print(traceback.format_exc())
