import traceback
import logging
from os.path import join
from datetime import date, timedelta, datetime

import networkx as nx
from pymongo import MongoClient
import pandas as pd

from libs.mongoLib import getContentDocsPerPlatform, getAllDocs, getMinMaxDay


def getGraphRequirments(collectionEnt, collectionLoc, collectionCont, platforms, timeLimits=None):
    # Convert to list due to multiple use
    contentList = list(getContentDocsPerPlatform(collectionCont, platforms))
    entitiesList = list(getAllDocs(collectionEnt))
    locationsList = list(getAllDocs(collectionLoc))

    minDay, maxDay = getMinMaxDay(collectionCont)
    temporalPeriod = [minDay + timedelta(days=x) for x in range((maxDay - minDay).days + 1)]

    if timeLimits is not None:
        #t todo test mek,.,
        logging.info(f'Limiting time from {timeLimits[0]} to {timeLimits[1]}')
        temporalPeriod = [d for d in temporalPeriod if d.year>=timeLimits[0] and d.year<=timeLimits[1]]
        originalContentLen = len(contentList)
        contentList = [c for c in contentList if c['timestamp'].year>=timeLimits[0] and c['timestamp'].year<=timeLimits[1]]
        logging.info(f'Dropped {originalContentLen-len(contentList)} pieces of content')

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


def createGraphEdges(G, contentList, nodesPerClass):
    for c in contentList:
        try:
            interactionDay = c['timestamp'].date()
            assert(interactionDay in nodesPerClass['time'])
            newEdges = [(c['_id'], interactionDay)]     # Day - Content

            if 'origin' in c.keys():
                location = c['origin']
                assert(location in nodesPerClass['location'])
                newEdges += [(c['_id'], location)]     # Content - Location

            if 'tags' in c.keys():
                tags = c['tags']
                assert(len([t for t in tags if t in nodesPerClass['tag']]) == len(tags))
                newEdges += [(c['_id'], t) for t in tags]        # Content - Tag

            G.add_edges_from(newEdges)

        except Exception as ex:
            print(traceback.format_exc())
            breakpoint()
    return G


def createGraphEdgesFaster(G, contentDf, nodesPerClass):
    try:
        # (Day)-(content) edges
        timeDf = contentDf['timestamp'].dt.normalize()
        timeEdges = list(timeDf.items())

        # (content)-Location edges
        locationDf = contentDf['origin'].dropna()
        locationEdges = list(locationDf.items())

        # (content-tags) edges
        tagDf = contentDf['tags'].dropna()
        tagDf = tagDf.apply(pd.Series).reset_index()
        tagDf = tagDf.melt(id_vars='_id')
        tagDf = tagDf.dropna()[['_id', 'value']]
        tagDf = tagDf.set_index('_id')
        tagDf = tagDf['value']
        tagEdges = list(tagDf.items())

        G.add_edges_from(timeEdges)
        G.add_edges_from(locationEdges)
        G.add_edges_from(tagEdges)

    except Exception as ex:
        print(traceback.format_exc())
        breakpoint()
    return G


def addNodeAttribute(G, data, platform=False):
    if platform is True:
        # Content nodes
        assert('contentDf' in data.keys())
        platformPerIdCont = data['contentDf']['platform'].to_dict()
        # Location nodes
        assert('locationDf' in data.keys())
        platformPerIdLoc = data['locationDf']['platform'].to_dict()

        # Make sure keys don't colide - this shouldn't be possible (IDs from content nodes != IDS from location nodes)
        assert(len([x for x in platformPerIdCont.keys() if x in platformPerIdLoc.keys()]) == 0)

        platformPerIdCont.update(platformPerIdLoc)
        nx.set_node_attributes(G, platformPerIdCont, 'platform')

    return G


if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir = '../data/'
    platforms = ['Facebook', 'Youtube', 'Google Search', 'Reddit', 'Twitter']
    minYear, maxYear = 2010, 2021
    # Facebook events force us to require this

    # Acquiring params
    create, loadFromMongo, loadFromOS, loadFromNeo4j = True, False, False, False
    # Storing Params
    saveToMongo, saveToOS, sendToNeo4j = True, True, True
    # Data to include
    includePlatform = True

    # Make sure we're only acquiring from one source
    assert(sum(1 for item in [create, loadFromMongo, loadFromOS, loadFromNeo4j] if item) == 1)

    try:
        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionEnt = db['entities']
        collectionLoc = db['locations']

        if create is True:
            # Creat the graph from nothing
            logging.info(f'Creating graph from nothing')

            data = getGraphRequirments(collectionEnt, collectionLoc, collectionCont, platforms, timeLimits=(minYear, maxYear))
            nodesPerClass = {
                'time': data['temporalPeriod'],
                'content': [x['_id'] for x in data['contentList']],
                'tag': [x['_id'] for x in data['entitiesList']],
                'location': [x['_id'] for x in data['locationsList']],
            }
            logging.info(f'Data acquired, creating graph (temporal period: {data["temporalPeriod"][0]} -> {data["temporalPeriod"][-1]})')

            data['contentDf'] = pd.DataFrame(data['contentList'])
            data['contentDf'].set_index('_id', inplace=True)

            G = nx.Graph()
            G = createGraphNodes(G, nodesPerClass)
            logging.info(f'Graph with {G.number_of_nodes()} nodes')
            # G = createGraphEdges(G, data['contentList'], nodesPerClass)
            G = createGraphEdgesFaster(G, data['contentDf'], nodesPerClass)
            logging.info(f'Graph with {G.number_of_edges()} edges')

            if includePlatform is True:
                data['locationDf'] = pd.DataFrame(data['locationsList'])
                data['locationDf'].set_index('_id', inplace=True)
                G = addNodeAttribute(G, data, platform=True)

        else:
            if loadFromMongo is True:
                # Load graph from collection
                pass    # TODO

            if loadFromOS is True:
                # Load graph from OS
                G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))

            if loadFromNeo4j is True:
                pass    # TODO

        if saveToMongo is True:
            pass    # TODO

        if saveToOS is True:
            nx.write_gpickle(G, join(baseDir, f'graph.gpickle'))

        if sendToNeo4j is True:
            pass    # TODO

    except Exception as ex:
        print(traceback.format_exc())
