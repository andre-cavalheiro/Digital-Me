from os import getcwd
import sys
sys.path.append(getcwd() + '/..')   # Add src/ dir to import path
import traceback
import logging
from os.path import join
from datetime import date, timedelta, datetime
from collections import Counter

import networkx as nx
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId

import libs.networkAnalysis as na
import libs.pandasLib as pl
from libs.mongoLib import getContentDocsPerPlatform, getAllDocs, getMinMaxDay
import libs.visualization as vz


def getGraphRequirments(collectionEnt, collectionLoc, collectionCont, platforms, timeLimits=None):
    # Convert to list due to multiple use
    contentList = list(getContentDocsPerPlatform(collectionCont, platforms))
    entitiesList = list(getAllDocs(collectionEnt))
    locationsList = list(getAllDocs(collectionLoc))

    minDay, maxDay = getMinMaxDay(collectionCont)
    temporalPeriod = [minDay + timedelta(days=x) for x in range((maxDay - minDay).days + 1)]

    if timeLimits is not None:
        logging.info(f'Limiting time from {timeLimits[0]} to {timeLimits[1]}')
        temporalPeriod = [d for d in temporalPeriod if d.year >= timeLimits[0] and d.year <= timeLimits[1]]

        droppedTimestamps, contentToDrop = 0, []
        for it, c in enumerate(contentList):
            timestampsToKeep = [t for t in c['timestamp'] if t.year >= timeLimits[0] and t.year <= timeLimits[1]]
            droppedTimestamps = len(c['timestamp']) - len(timestampsToKeep)
            if len(timestampsToKeep) == 0:
                contentToDrop.append(it)
            else:
                c['timestamp'] = timestampsToKeep

        contentList = [c for it, c in enumerate(contentList) if it not in contentToDrop]
        logging.info(f'Dropped {droppedTimestamps} content timestamps (outside of temporal range) - '
                     f'resulting in deleting {len(contentToDrop)} pieces of content entirely.')

    return {
        'contentList': contentList,
        'entitiesList': entitiesList,
        'locationsList': locationsList,
        'temporalPeriod': temporalPeriod,
    }


def createGraphNodes(G, nodesPerClass):
    for clss, listOfNodes in nodesPerClass.items():
        G.add_nodes_from(listOfNodes, nodeClass=clss)
    return G


def createGraphEdges(G, temporalPeriod, contentDf, nodesPerClass):
    '''
    Adds the following types of connections to the graph: 
        (Day)-[temporal]-(Day)
        (Day)-[action]-(Content)
        (Content)-[$Variable]-(Location); $Variable={authorship, placed, impliedMention, inherentMention}
        (Content)-[$Variable]-(Tags); $Mention={impliedMention, inherentMention}
    :param G: 
    :param temporalPeriod: 
    :param contentDf: 
    :param nodesPerClass: 
    :return:
    '''
    try:
        # (Day)-(Day)
        timeEdges = [(temporalPeriod[i], temporalPeriod[i+1]) for i in range(len(temporalPeriod)-1)]
        logging.info(f'> Time Edges {len(timeEdges)}')
        
        # (Day)-(Content) edges
        actionDf = pl.unrollListAttr(contentDf.reset_index(), 'timestamp', ['_id'])
        actionDf = actionDf.set_index('_id')['value']
        actionEdges = list(actionDf.items())
        # Make sure every tail is already a node in the graph
        assert(len([e[1] for e in actionEdges if e[1] not in nodesPerClass['time']]) == 0)
        logging.info(f'> Action Edges {len(actionEdges)}')

        # (Content)-(Location) edges
        locationsDf = pl.unrollListOfDictsAttr(contentDf.reset_index(), 'locations', ['_id'])
        locationsDf.set_index('_id', inplace=True)
        sourceEdgeTypes = locationsDf['relationshipType'].tolist()
        locationEdges = list(locationsDf['label'].items())
        assert(len([e[1] for e in locationEdges if e[1] not in nodesPerClass['spatial']]) == 0)
        logging.info(f'> Source Edges {len(locationEdges)}')

        # (Content)-(Tags) edges
        tagDf = pl.unrollListOfDictsAttr(contentDf.reset_index(), 'tags', ['_id'])
        tagDf.set_index('_id', inplace=True)
        tagEdgeTypes = tagDf['relationshipType'].tolist()
        tagEdges = list(tagDf['label'].items())
        assert(len([e[1] for e in tagEdges if e[1] not in nodesPerClass['tag']]) == 0)
        logging.info(f'> Tag Edges {len(tagEdges)}')

        # Add them all to the graph
        G.add_edges_from(timeEdges, edgeClass='temporal')
        G.add_edges_from(actionEdges, edgeClass='action')
        G.add_edges_from(locationEdges, edgeClass=sourceEdgeTypes)
        G.add_edges_from(tagEdges, edgeClass=tagEdgeTypes)

    except Exception as ex:
        print(traceback.format_exc())
        breakpoint()
    return G


def addNodeAttributes(G, data, platform=False, contentType=False, locationType=False):
    if platform is True:
        # Regarding content nodes
        assert('contentDf' in data.keys())
        platformPerIdCont = data['contentDf']['platform'].to_dict()

        # Regarding location nodes
        assert('locationDf' in data.keys())
        platformPerIdLoc = data['locationDf']['platform'].to_dict()

        # Make sure keys don't collide - this shouldn't be possible (IDs from content nodes != IDS from location nodes)
        assert(len([x for x in platformPerIdCont.keys() if x in platformPerIdLoc.keys()]) == 0)

        platformPerIdCont.update(platformPerIdLoc)
        nx.set_node_attributes(G, platformPerIdCont, 'platform')

    if contentType is True:
        assert('contentDf' in data.keys())
        contentType = data['contentDf']['type'].to_dict()
        nx.set_node_attributes(G, contentType, 'contentType')

    if locationType is True:
        assert('locationDf' in data.keys())
        locationType = data['locationDf']['type'].to_dict()
        nx.set_node_attributes(G, locationType, 'locationType')

    return G


if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir = '../../data/'
    platforms = ['Facebook', 'YouTube', 'Google Search', 'Reddit', 'Twitter']     # ,
    minYear, maxYear = 2019, 2019

    try:
        # Set up DB
        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionEnt = db['entities']
        collectionLoc = db['locations']

        logging.info(f'Querying Data')
        data = getGraphRequirments(collectionEnt, collectionLoc, collectionCont, platforms, timeLimits=(minYear, maxYear))
        nodesPerClass = {
            'time': data['temporalPeriod'],
            'content': [x['_id'] for x in data['contentList']],
            'tag': [x['_id'] for x in data['entitiesList']],
            'spatial': [x['_id'] for x in data['locationsList']],
        }
        logging.info(f'Data acquired, creating graph (temporal period: {data["temporalPeriod"][0]} -> {data["temporalPeriod"][-1]})')

        # Transform lists to dataframe for faster operations
        data['contentDf'] = pd.DataFrame(data['contentList']).set_index('_id')
        data['contentDf'].timestamp = data['contentDf'].timestamp.apply(lambda x: [d.date() for d in x])
        data['locationDf'] = pd.DataFrame(data['locationsList']).set_index('_id')
        data['tagDf'] = pd.DataFrame(data['entitiesList']).set_index('_id')
        
        # Creat the graph
        G = nx.Graph()
        G = createGraphNodes(G, nodesPerClass)
        logging.info(f'Graph with {G.number_of_nodes()} nodes')
        G = createGraphEdges(G, data['temporalPeriod'], data['contentDf'], nodesPerClass)
        logging.info(f'Graph with {G.number_of_edges()} edges')

        # Ensure we have a single component
        # Since we are limiting time, we may include some nodes (tags/locations) that are connected to content nodes
        # which don't end up in the graph. This solves this - todo
        G = na.getOnlyConnectedGraph(G)

        locationsDf = pl.unrollListOfDictsAttr(data['contentDf'].reset_index(), 'locations', ['_id'])
        locationsDf.set_index('_id', inplace=True)
        tagDf = pl.unrollListOfDictsAttr(data['contentDf'].reset_index(), 'tags', ['_id'])
        tagDf.set_index('_id', inplace=True)

        datePerMonthIndex = {}
        for date in nodesPerClass['time']:
            month = date.strftime("%b")
            if month in datePerMonthIndex.keys():
                datePerMonthIndex[month].append(date)
            else:
                datePerMonthIndex[month] = [date]

        datePerMonthIndex = {y: sorted(days) for y, days in datePerMonthIndex.items()}
        plotData, memberships = [], []

        for month, sortedListOfTimestamps in datePerMonthIndex.items():
            # sortedListOfTimestamps = nodesPerClass['time'][:100]
            entities, entityMembership = [], {'tag': [], 'location': []}

            for i in range(len(sortedListOfTimestamps)-1):
                day1, day2 = sortedListOfTimestamps[i], sortedListOfTimestamps[i+1]
                length = 4
                paths = nx.all_simple_paths(G, day1, day2, cutoff=length)
                paths = [p for p in paths if len(p)==length+1]
                for p in paths:
                    entityId = p[2]
                    entities.append(entityId)

            entityFreqs = dict(Counter(entities))
            entityLabels = {}
            for entityId in entityFreqs.keys():
                if entityId in data['tagDf'].index:
                    label = data['tagDf'].loc[entityId]['normalizedForms'][0]
                    entityLabels[entityId] = label
                    entityMembership['tag'].append(label)
                elif entityId in data['locationDf'].index:
                    locationType = data['locationDf'].loc[entityId]['type']
                    label = data['locationDf'].loc[entityId]['label']

                    entityLabels[entityId] = label
                    # entityMembership['location'].append(label)
                    if locationType not in entityMembership.keys():
                        entityMembership[locationType] = [label]
                    else:
                        entityMembership[locationType].append(label)
                else:
                    print(f'No match for {entityId}')
                    continue

            frequenciesPerLabel = {entityLabels[id]: entityFreqs[id] for id in entityLabels.keys()}
            print(frequenciesPerLabel)
            plotData.append(frequenciesPerLabel)
            memberships.append(entityMembership)
            print(month + 'DONE')

            '''
            if month == 'Feb':
                break
            '''

        print('PLOTTING')
        vz.smallMultipleWordCloud(plotData, groupMemberships=memberships, colorLabels={
            'tag': "#483d8b",
            # 'location': "#e62525",
            'YouTube Channel': "#065915",
            'Facebook Account': "#bb0b60",
            'URL': "#bb480b",
            'Facebook Location': "#a6a6a6",
            'Subreddit': "#6de182",
        })

    except Exception as ex:
        print(traceback.format_exc())
