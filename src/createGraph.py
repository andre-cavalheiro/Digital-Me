import traceback
import logging
from os.path import join
from datetime import date, timedelta, datetime

import networkx as nx
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId

import libs.networkAnalysis as na
from libs.mongoLib import getContentDocsPerPlatform, getAllDocs, getMinMaxDay


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
        (Day)-(Day)
        (Day)-(Content)
        (Content)-(Location)
        (Content)-(Tags)
    :param G: 
    :param temporalPeriod: 
    :param contentDf: 
    :param nodesPerClass: 
    :return: 
    '''
    try:
        # (Day)-(Day)
        timeEdges = [(temporalPeriod[i], temporalPeriod[i+1]) for i in range(len(temporalPeriod)-1)]
        
        # (Day)-(Content) edges
        timeDf = contentDf['timestamp'].apply(pd.Series).reset_index().melt(id_vars='_id').dropna()[['_id', 'value']].set_index('_id')['value']
        timestampEdges = list(timeDf.items())

        # Make sure every tail is already a node in the graph
        assert(len([e[1] for e in timestampEdges if e[1] not in nodesPerClass['time']]) == 0)

        # (Content)-(Location) edges
        locationDf = contentDf['locations'].dropna()
        locationDf = locationDf.apply(pd.Series).reset_index().melt(id_vars='_id').dropna()[['_id', 'value']].set_index('_id')['value']
        locationEdges = list(locationDf.items())
        assert(len([e[1] for e in locationEdges if e[1] not in nodesPerClass['location']]) == 0)

        # (Content)-(Tags) edges
        tagDf = contentDf['tags'].dropna()
        tagDf = tagDf.apply(pd.Series).reset_index().melt(id_vars='_id').dropna()[['_id', 'value']].set_index('_id')['value']
        tagEdges = list(tagDf.items())
        assert(len([e[1] for e in tagEdges if e[1] not in nodesPerClass['tag']]) == 0)

        # Add them all to the graph
        G.add_edges_from(timeEdges)
        G.add_edges_from(timestampEdges)
        G.add_edges_from(locationEdges)
        G.add_edges_from(tagEdges)

    except Exception as ex:
        print(traceback.format_exc())
        breakpoint()
    return G


def addNodeAttributes(G, data, platform=False, contentType=False):
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

    return G


if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir = '../data/'
    platforms = ['Facebook', 'YouTube', 'Google Search', 'Reddit', 'Twitter']
    minYear, maxYear = 2009, 2021
    # Facebook events force us to require this

    # Acquiring params
    create, loadFromOS, loadFromMongo, loadFromNeo4j = True, False, False, False
    # Storing Params
    saveToMongo, saveToOS, sendToNeo4j = True, True, True
    # Data to include
    includePlatform, includeContentType = True, True

    # Make sure we're only acquiring from one source
    assert(sum(1 for item in [create, loadFromMongo, loadFromOS, loadFromNeo4j] if item) == 1)

    try:
        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionEnt = db['entities']
        collectionLoc = db['locations']

        if create is True:
            logging.info(f'Creating graph from nothing')

            data = getGraphRequirments(collectionEnt, collectionLoc, collectionCont, platforms, timeLimits=(minYear, maxYear))
            nodesPerClass = {
                'time': data['temporalPeriod'],
                'content': [x['_id'] for x in data['contentList']],
                'tag': [x['_id'] for x in data['entitiesList']],
                'location': [x['_id'] for x in data['locationsList']],
            }
            logging.info(f'Data acquired, creating graph (temporal period: {data["temporalPeriod"][0]} -> {data["temporalPeriod"][-1]})')

            # Transform lists to dataframe for faster operations
            data['contentDf'] = pd.DataFrame(data['contentList']).set_index('_id')
            # data['contentDf'].timestamp = data['contentDf'].timestamp.dt.date
            data['contentDf'].timestamp = data['contentDf'].timestamp.apply(lambda x: [d.date() for d in x])
            data['locationDf'] = pd.DataFrame(data['locationsList']).set_index('_id')

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

            G = addNodeAttributes(G, data, platform=includePlatform, contentType=includeContentType)

        else:
            if loadFromMongo is True:
                # Load graph from collection
                pass    # TODO

            if loadFromOS is True:
                logging.info(f'Loading graph from OS')
                # Load graph from OS
                G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))

            if loadFromNeo4j is True:
                pass    # TODO

        if saveToMongo is True:
            pass    # TODO

        if saveToOS is True:
            logging.info(f'Saved to OS')
            nx.write_gpickle(G, join(baseDir, f'graph.gpickle'))

        if sendToNeo4j is True:
            logging.info(f'Saved to neo4j')
            nx.write_graphml(G,  join(baseDir, f'graph.graphml'))

    except Exception as ex:
        print(traceback.format_exc())
