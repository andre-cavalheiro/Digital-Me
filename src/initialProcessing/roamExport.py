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
from libs.mongoLib import getContentDocsPerPlatform, getAllDocs, getMinMaxDay
from initialProcessing.createGraph import getGraphRequirments


if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir, outputDir = '../../data/', '../../data/Roam'
    platforms = ['Twitter', 'Facebook', 'YouTube', 'Google Search', 'Reddit']
    minYear, maxYear = 2009, 2021
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
        data['contentDf'] = data['contentDf'][['platform', 'type', 'timestamp', 'body', 'tags', 'locations']]

        data['locationDf'] = pd.DataFrame(data['locationsList']).set_index('_id')
        data['entityDf'] = pd.DataFrame(data['entitiesList']).set_index('_id')

        # Create one Roam page per Day
        def getDayFinisher(myDate):
            date_suffix = ["th", "st", "nd", "rd"]

            if myDate % 10 in [1, 2, 3] and myDate not in [11, 12, 13]:
                return date_suffix[myDate % 10]
            else:
                return date_suffix[0]
        dateLabels = {d: d.strftime(f'%B %dRR, %Y').replace('RR', getDayFinisher(d.day)) for d in
                      data['temporalPeriod']}

        # Organize data
        df = data['contentDf']
        df.reset_index(inplace=True)

        # FIXME - I have a function for this now!
        auxdf = df['timestamp'].apply(pd.Series).reset_index()
        auxdf = auxdf.melt(id_vars='index')
        auxdf = auxdf.dropna()[['index', 'value']]
        auxdf = auxdf.set_index('index')
        df = pd.merge(auxdf, df, left_index=True, right_index=True)
        df = df.reset_index(drop=True).rename(columns={'value': 'day'})

        uniqueDays = df.day.unique()
        payloadForFiles = {}

        for day in uniqueDays:
            dayLabel, year = dateLabels[day], day.year

            if year not in payloadForFiles.keys():
                payloadForFiles[year] = {}

            payloadForFiles[year][dayLabel] = {}

            specificDf = df[df.day == day]
            for _, dt in specificDf.iterrows():
                platform = dt['platform']
                type = dt['type']
                datapoint = {
                    'body': dt['body'] if isinstance(dt['body'], str) else '',
                    'tags': dt['tags'] if isinstance(dt['tags'], list) else [],
                    'source': dt['locations'] if isinstance(dt['locations'], list) else [],
                }
                if platform not in payloadForFiles[year][dayLabel].keys():
                    payloadForFiles[year][dayLabel][platform] = {type: [datapoint]}
                else:
                    if type  in payloadForFiles[year][dayLabel][platform].keys():
                        payloadForFiles[year][dayLabel][platform][type].append(datapoint)
                    else:
                        payloadForFiles[year][dayLabel][platform][type] = [datapoint]

        if singleFile is True:
            # Create a single file
            for year, days in payloadForFiles.items():
                mdFile = MdUtils(file_name=join(outputDir, f'history{year}.md'))

                for day, dt in days.items():
                    logging.info(f'{day}')
                    auxFileContent = []
                    for platform, content in dt.items():
                        # mdFile.new_header(level=2, title=platform)
                        auxFileContent.append(f'## {platform}')

                        placeholder = []
                        for contentType, payloads in content.items():
                            # mdFile.new_header(level=3, title=contentType)
                            placeholder.append(f'### {contentType}')
                            datapoints = []
                            for p in payloads:
                                tags = [f" [[{data['entityDf'].loc[s, 'mentionForms'][0]}]]" for s in p['tags']]    # Choose first mention form by default
                                sources = [f" [[{data['locationDf'].loc[s, 'label']}]]" for s in p['source']]

                                attach = []
                                if len(tags) > 0:
                                    attach.append(' **Tags:**')
                                    attach.append(tags)
                                if len(sources) > 0:
                                    attach.append(' **Sources:**')
                                    attach.append(sources)

                                datapoints.append([p['body'].replace('\n', ' '), attach])
                            placeholder.append(datapoints)

                        auxFileContent.append(placeholder)

                    mdFile.new_list([f'[[{day}]]', auxFileContent])
                mdFile.create_md_file()
        else:
            # Create one file per day
            for year, days in payloadForFiles.items():
                for day, dt in days.items():
                    logging.info(f'{day}')
                    mdFile = MdUtils(file_name=join(outputDir, f'{day}.md'), title=f'{day}')

                    auxFileContent = []
                    for platform, content in dt.items():
                        # mdFile.new_header(level=2, title=platform)
                        auxFileContent.append(f'{platform}')

                        placeholder = []
                        for contentType, payloads in content.items():
                            # mdFile.new_header(level=3, title=contentType)
                            placeholder.append(f'{contentType}')
                            datapoints = []
                            for p in payloads:
                                tags = [f" [[{data['entityDf'].loc[s, 'mentionForms'][0]}]]" for s in
                                        p['tags']]  # Choose first mention form by default
                                sources = [f"[[{data['locationDf'].loc[s, 'label']}]]" for s in p['source']]

                                attach = []
                                if len(tags) > 0:
                                    attach.append(' **Tags:**')
                                    attach.append(tags)
                                if len(sources) > 0:
                                    attach.append(' **Sources:**')
                                    attach.append(sources)

                                datapoints.append([p['body'].replace('\n', ' '), attach])
                            placeholder.append(datapoints)

                        auxFileContent.append(placeholder)

                    mdFile.new_list(auxFileContent)
                    mdFile.create_md_file()

    except Exception as ex:
        print(traceback.format_exc())