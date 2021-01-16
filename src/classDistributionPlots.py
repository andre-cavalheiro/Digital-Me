import traceback
import logging
from os.path import join
from datetime import date, timedelta, datetime
from collections import Counter
from itertools import chain
from datetime import datetime

import networkx as nx
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pymongo import MongoClient
import plotly.express as px
from plotly.offline import plot

import libs.networkAnalysis as na
from libs.mongoLib import updateContentDocs
import libs.visualization as vz

if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir = '../data/'
    outputDir = join(baseDir, 'Plots')

    try:

        G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))
        outputDf = pd.DataFrame(columns=['Class', 'Subclass', 'Count'])

        # Content
        contentSubClasses = [G.nodes[n]['contentType'] for n in G.nodes() if G.nodes[n]['nodeClass'] == 'content']
        subClassCount = Counter(contentSubClasses)
        auxDf = pd.DataFrame.from_dict(subClassCount, orient='index', columns=['Count'])
        auxDf.index.rename('Subclass', inplace=True)
        auxDf.reset_index(inplace=True)
        auxDf['Class'] = 'Content'
        outputDf = outputDf.append(auxDf)

        # Time
        timeNodes = len([1 for n in G.nodes() if G.nodes[n]['nodeClass'] == 'time'])
        outputDf = outputDf.append({
            'Class': 'Time',
            'Subclass': 'Time',
            'Count': timeNodes,
        }, ignore_index=True)

        # Location
        locationNodes = len([1 for n in G.nodes() if G.nodes[n]['nodeClass'] == 'location'])
        outputDf = outputDf.append({
            'Class': 'Location',
            'Subclass': 'Location',
            'Count': locationNodes,
        }, ignore_index=True)

        # Tags
        tagNodes = len([1 for n in G.nodes() if G.nodes[n]['nodeClass'] == 'tag'])
        outputDf = outputDf.append({
            'Class': 'Tag',
            'Subclass': 'Extracted Entities',
            'Count': tagNodes,
        }, ignore_index=True)

        # Make plot
        countTotal = outputDf['Count'].sum()
        outputDf['percentage'] = outputDf.apply(lambda row: 100 * row['Count'] / countTotal, axis=1)

        fig = px.treemap(outputDf,
                         path=['Class', 'Subclass'],
                         values='Count',
                         hover_data=['percentage'],
                         custom_data=['percentage'])

        fig.data[0].textinfo = 'label+text+value+percent root+percent parent'
        fig.layout.hovermode = False

        # fig.layout.hovermode = False
        plot(fig, filename=join(outputDir, f'treemap-nodes.html'))

        """
        nodeHierarchy = {
            'content': ['SEARCH_QUERY', 'WEBPAGE', 'YOUTUBE_VIDEO'],
            'location': ['WEBSITE', 'YOUTUBE_CHANNEL'],
            'time': ['TIME'],
            'tags': ['EXTRACTED_ENTITY'],  # kind of a special case...
        }
        
        invertedHierarchy = {}
        hierarchicalIndex = [[], []]
        for k, v in nodeHierarchy.items():  # 1st level
            for l in v:  # 2nd level
                hierarchicalIndex[0].append(k)
                hierarchicalIndex[1].append(l)
                invertedHierarchy[l] = k

        dfIndex = pd.MultiIndex.from_arrays(hierarchicalIndex, names=('class', 'subclass'))
        outputDf = pd.DataFrame(0, dfIndex, columns=['count'])

        countTotal = outputDf['count'].sum()
        outputDf['percentage'] = outputDf.apply(lambda row: 100 * row['count'] / countTotal, axis=1)
        outputDf = outputDf.reset_index()

        fig = px.treemap(outputDf,
                         path=['class', 'subclass'],
                         values='count',
                         hover_data=['percentage'],
                         custom_data=['percentage'])

        fig.data[0].textinfo = 'label+text+value+percent root+percent parent'
        fig.layout.hovermode = False

        # fig.layout.hovermode = False
        plot(fig, filename=join(outputDir, f'treemap-nodes.html'))
        """

    except Exception as ex:
        print(traceback.format_exc())
