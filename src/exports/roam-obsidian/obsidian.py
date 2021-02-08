from os import getcwd
import sys
sys.path.append(getcwd() + '/..')   # Add src/ dir to import path
import traceback
import logging
from os.path import join
from datetime import date, timedelta, datetime

import networkx as nx
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
from mdutils.mdutils import MdUtils

import libs.networkAnalysis as na
import libs.pandasLib as pl
from libs.mongoLib import getContentDocsPerPlatform, getAllDocs, getMinMaxDay
from initialProcessing.createGraph import getGraphRequirments

from libs.osLib import loadYaml

def getDayFinisher(myDate):
    date_suffix = ["th", "st", "nd", "rd"]

    if myDate % 10 in [1, 2, 3] and myDate not in [11, 12, 13]:
        return date_suffix[myDate % 10]
    else:
        return date_suffix[0]


if __name__ == '__main__':

    # Set up logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    
    # Load config
    configDir = '../../configs/'
    config = loadYaml(join(configDir, 'export.yaml'))

    platforms = config['platforms']
    exportKeys = {plt: dt for plt, dt in config['keysToExport'].items()}
    minYear, maxYear = 2009, 2020
    singleFile = False

    try:
        # Set up DB
        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionEnt = db['entities']
        collectionLoc = db['locations']

        logging.info(f'Loading data from DB')

        data = getGraphRequirments(collectionEnt, collectionLoc, collectionCont, platforms,
                                   timeLimits=(minYear, maxYear))
        nodesPerClass = {
            'time': data['temporalPeriod'],
            'content': [x['_id'] for x in data['contentList']],
            'tag': [x['_id'] for x in data['entitiesList']],
            'spatial': [x['_id'] for x in data['locationsList']],
        }
        logging.info(
            f'Data acquired, creating graph (temporal period: {data["temporalPeriod"][0]} -> {data["temporalPeriod"][-1]})')

        # Transform lists to dataframe for faster operations
        data['contentDf'] = pd.DataFrame(data['contentList']).set_index('_id')
        data['contentDf'].timestamp = data['contentDf'].timestamp.apply(lambda x: [d.date() for d in x])
        # data['contentDf'] = data['contentDf'][['platform', 'type', 'timestamp', 'body', 'tags', 'locations']]

        data['locationDf'] = pd.DataFrame(data['locationsList']).set_index('_id')
        data['entityDf'] = pd.DataFrame(data['entitiesList']).set_index('_id')
        dateLabels = {d: d.strftime(f'%B %dRR, %Y').replace('RR', getDayFinisher(d.day)) for d in
                      data['temporalPeriod']}

        # Organize data
        df = data['contentDf']
        df.reset_index(inplace=True)
        df = pl.unrollListAttr(df.reset_index(), 'timestamp', newAttrName='day', otherColumns=df.drop('timestamp', axis=1).columns.tolist())
        uniqueDays = df.day.unique()
        payloadForFiles = {}    # Year | Day | Platform | ContentType | ContentList
        for day in uniqueDays:

            dayLabel, year = dateLabels[day], day.year

            # Init fileContent_aux1s
            if year not in payloadForFiles.keys():
                payloadForFiles[year] = {}
            payloadForFiles[year][dayLabel] = {}

            specificDf = df[df.day == day]

            # Iterate content from this day
            for _, dt in specificDf.iterrows():
                platform = dt['platform']
                type = dt['type']
                try:
                    payloadKey = exportKeys[platform][type]     # FIXME
                    r=1
                    datapoint = {
                        'body': dt[payloadKey[0]] if isinstance(dt['body'], str) else '',
                        'tags': dt['tags'] if 'tags' in dt.keys() and isinstance(dt['tags'], list) else [],     # Lists of {id: , relationshipType}
                        'source': dt['locations'] if isinstance(dt['locations'], list) else [],  # Lists of {id: , relationshipType}
                    }
                except Exception as ex:
                    print(traceback.format_exc())
                    breakpoint()

                if platform not in payloadForFiles[year][dayLabel].keys():
                    payloadForFiles[year][dayLabel][platform] = {type: [datapoint]}
                else:
                    if type in payloadForFiles[year][dayLabel][platform].keys():
                        payloadForFiles[year][dayLabel][platform][type].append(datapoint)
                    else:
                        payloadForFiles[year][dayLabel][platform][type] = [datapoint]

        # Create one file per day
        for year, days in payloadForFiles.items():
            for day, dt in days.items():
                logging.info(f'{day}')

                # Create file
                mdFile = MdUtils(file_name=join(config['outputDir'], f'{day}.md'), title=f'{day}')

                for platform, content in dt.items():
                    # Write Platform
                    mdFile.new_header(level=1, title=platform)
                    fileContent_aux1 = []

                    for contentType, contentList in content.items():
                        # Write content Type
                        mdFile.new_header(level=2, title=contentType)
                        fileContent_aux2 = []

                        for c in contentList:
                            body = c['body'].replace('\n', ' ')

                            fileContent_aux3 = []

                            # Sort connections by content Type
                            connections = {}
                            for attach in c['tags']:
                                if attach['relationshipType'] not in connections.keys():
                                    connections[attach['relationshipType']] = []

                                tagId = attach['label']
                                tagMentionForms = data['entityDf'].loc[tagId, 'mentionForms']

                                # connections[attach['relationshipType']].append(tagMentionForms[0])  # FIXME

                                for t in tagMentionForms:
                                    if t in body:
                                        body.replace(t, f'[[{t}]]')
                                        connections[attach['relationshipType']].append(f'[[{t}]]')
                                        break

                            for attach in c['source']:
                                if attach['relationshipType'] not in connections.keys():
                                    connections[attach['relationshipType']] = []
                                sourceId = attach['label']
                                sourceLabel = data['locationDf'].loc[sourceId, 'label']
                                connections[attach['relationshipType']].append(f'[[{sourceLabel}]]')

                                if sourceLabel in body:
                                    body.replace(sourceLabel, f'[[{sourceLabel}]]')
                                    connections[attach['relationshipType']].append(sourceLabel)
                                    break

                            # Write connections
                            for connectionType, targetEntities in connections.items():
                                fileContent_aux3.append(f'**{connectionType.capitalize()}**')
                                fileContent_aux3.append(targetEntities)

                            fileContent_aux2.append(body)
                            fileContent_aux2.append(fileContent_aux3)

                        mdFile.new_list(fileContent_aux2)

                mdFile.create_md_file()
            break

    except Exception as ex:
        print(traceback.format_exc())