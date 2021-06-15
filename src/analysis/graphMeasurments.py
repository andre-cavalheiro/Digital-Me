from os import getcwd
import sys

sys.path.append(getcwd() + '/..')  # Add src/ dir to import path
import traceback
import logging
from os.path import join
from datetime import date, timedelta, datetime
import json

import networkx as nx
import pandas as pd
import numpy as np

from libs.osLib import loadYaml
from libs.networkAnalysis import *
from libs.mongoLib import updateContentDocs, getFromId, getContentDocsPerPlatform

labels = {
  'day': '',
  'week': '-week',
  'month': '-month',
}

if __name__ == '__main__':
  root = logging.getLogger()
  root.setLevel(logging.DEBUG)

  # Load config
  configDir = '../../configs/'
  config = loadYaml(join(configDir, 'main.yaml'))
  period = 'week'   # 'day', 'week', 'month'
  period_label = labels[period]

  try:
    stats = {}

    # Load graph from OS
    G = nx.read_gpickle(join(config['dataDir'], f'graph{period_label}.gpickle'))

    # Get nodes with highest degrees
    from pymongo import MongoClient

    client = MongoClient()
    db = client['digitalMe']
    collectionCont = db['content']
    collectionEnt = db['entities']
    collectionSource = db['locations']

    degrees = sorted([{'id': node, 'degree': val} for (node, val) in G.degree() if not isinstance(node, str)], key=lambda k: -k['degree'])
    db_info = [getFromId(i['id'], collectionCont, collectionEnt, collectionSource)[0] for i in degrees]

    # twitter = [(i, degrees[i]['degree'], d) for i, d in enumerate(db_info) if 'platform' in d.keys() and d['platform'] == 'Twitter']

    reddit = [(i, degrees[i]['degree'], d) for i, d in enumerate(db_info) if 'platform' in d.keys() and d['platform'] == 'Reddit']

    tags = [(i, degrees[i]['degree'], d) for i, d in enumerate(db_info) if 'normalizedForms' in d.keys()]

    # Count time nodes and action edges
    amountOfTimeNodes = len([n for n in G.nodes(data=True) if n[1]['nodeClass'] == 'time'])
    amountOfActionEdges = len([e for e in G.edges(data=True) if e[2]['edgeClass'] == 'action'])
    print(f'Count of time nodes: {amountOfTimeNodes}')
    print(f'Count of action edges: {amountOfActionEdges}')

    # Degree stats
    degrees = [d for n, d in G.degree()]
    stats['avgDegree'], stats['maxDegree'], stats['minDegree'], stats['percentile-1'], stats['percentile-2'], \
    stats['percentile-3'] = getStatistics(degrees)
    stats['alpha'], stats['sigma'] = fitPowerLaw(degrees)
    print('Step one done')

    stats['density'] =nx.density(G)
    print('Step one and a half done')
    print(stats)
    exit()
    stats['diameter'] = diameter(G)

    print('Step two done')
    stats['transitivity'] = transitivity(G)

    print('Step three done')
    stats['average_path_len'] = averagePathLen(G)

    print('All done!')

    print(stats)
    with open(f'output{period_label}.json', 'w+') as f:
      json.dump(stats, f)

  except Exception as ex:
    print(traceback.format_exc())
