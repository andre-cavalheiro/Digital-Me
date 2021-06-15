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

import libs.networkAnalysis as na
import libs.pandasLib as pl
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
    minYear, maxYear = 2009, 2010
    # Facebook events force us to require this

    # Acquiring params
    create, loadFromOS, loadFromMongo = True, False, False
    # Storing Params
    saveToMongo, saveToOS, saveAsGraphML = False, False, False
    # Data to include
    includePlatform, includeContentType, includeLocationType = True, True, True

    # Make sure we're only acquiring data from one source
    assert(sum(1 for item in [create, loadFromMongo, loadFromOS] if item) == 1)

    try:
        # Set up DB
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
                'spatial': [x['_id'] for x in data['locationsList']],
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

            G = addNodeAttributes(G, data, platform=includePlatform, contentType=includeContentType, locationType=includeLocationType)

        else:
            if loadFromMongo is True:
                # Load graph from collection
                pass    # TODO

            if loadFromOS is True:
                logging.info(f'Loading graph from OS')
                # Load graph from OS
                G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))

        if saveToMongo is True:
            pass    # TODO

        if saveToOS is True:
            logging.info(f'Saved to OS')
            nx.write_gpickle(G, join(baseDir, f'graph.gpickle'))

        if saveAsGraphML is True:
            # Make it viable for Neo4j import
            graphML = G.copy()

            # Add node labels (classes)
            classes = nx.get_node_attributes(graphML, 'nodeClass')
            classes = {k: f':{v.upper()}' for k, v in classes.items()}
            nx.set_node_attributes(graphML, classes, 'labels')
            # print(graphML.nodes[list(classes.keys())[0]])
            # for n, dt in graphML.nodes(data=True):
            #    del dt['nodeClass']

            # Add edge labels (classes)
            classes = nx.get_edge_attributes(graphML, 'edgeClass')
            nx.set_edge_attributes(graphML, classes, 'label')
            # for n, dt in graphML.edges(data=True):
            #    del dt['edgeClass']

            nx.write_graphml(graphML,  join(baseDir, f'graph.graphml'))
            logging.info(f'Saved as graphML')

    except Exception as ex:
        print(traceback.format_exc())
