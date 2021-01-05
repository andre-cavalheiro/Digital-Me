import traceback

from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize

from libs.mongoLib import updateContentDocs, getContentDocsWithEntities


def standardizeIds(df):

    linkedDf = df[df.entityId.str.startswith('Q')]
    uniqueQIDs = linkedDf.entityId.unique()
    normalizedTags = df.normalized.unique()

    df['droppedQIDs'] = None

    for tag in normalizedTags:
        specificDf = df.loc[df.normalized == tag]

        # If there's only one ID associated with that tag, it's all good.
        if len(specificDf.entityId.unique()) == 1:
            continue
        else:
            conflictingIds = specificDf.entityId.unique()
            QIDs = [id for id in conflictingIds if id in uniqueQIDs]

            if len(QIDs) == 1:
                # If there's exactly one QID among the conflicting ones, that one subs the remaining.
                df.loc[df.normalized == tag].entityId = QIDs[0]
            elif len(QIDs) == 0:
                # If there's no QIDs among them choose any of the forged IDs and standardize that one.
                df.loc[df.normalized == tag, 'entityId'] = conflictingIds[0]
            else:
                # If there are conflicting QIDs, choose the first one by default but keep the other ones for possible usage
                df.loc[df.normalized == tag, 'entityId'] = QIDs[0]
                for index, row in df.loc[df.normalized == tag].iterrows():
                    df.at[index, 'droppedQIDs'] = QIDs
    return df


def prepareEntityCollection(df):
    output = []
    uniqueIds = df.index.unique()
    for id in uniqueIds:
        specificDf = df.loc[id].to_frame().transpose() if isinstance(df.loc[id], pd.Series) else df.loc[id]

        associatedContent = specificDf._id.unique().tolist()
        types = specificDf.type.unique().tolist()
        mentionForms = specificDf.mention.unique().tolist()
        normalizedForms = specificDf.normalized.unique().tolist()

        newDataPoint = {
            'id': id,
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

    try:

        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionEnt = db['entities']

        # Query DB for content with extracted entities
        df = getContentDocsWithEntities(collectionCont)

        # Ensure entities with equal names have the same ID
        df = standardizeIds(df)

        # Prepare data for DB
        df.set_index('entityId', inplace=True)
        entityCollectionData = prepareEntityCollection(df)

        # Save it to DB
        insertedDocs = collectionEnt.insert_many(entityCollectionData)
        insertedIds = insertedDocs.inserted_ids

        # Update content docs with tags
        contentDocsPayload = {}
        for info, entityId in zip(entityCollectionData, insertedIds):
            for contentDoc in info['associatedContent']:
                if contentDoc not in contentDocsPayload.keys():
                    contentDocsPayload[contentDoc] = [entityId]
                else:
                    contentDocsPayload[contentDoc].append(entityId)
        updateContentDocs(collectionCont, 'tags', contentDocsPayload)

    except Exception as ex:
        print(traceback.format_exc())
