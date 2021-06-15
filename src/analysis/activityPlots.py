from os import getcwd
import sys
sys.path.append(getcwd() + '/..')   # Add src/ dir to import path
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

import libs.networkAnalysis as na
from libs.mongoLib import updateContentDocs
import libs.visualization as vz
from libs.osLib import loadYaml

if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Load config
    configDir = '../../configs/'
    config = loadYaml(join(configDir, 'main.yaml'))

    try:
        G = nx.read_gpickle(join(config['dataDir'], f'graph.gpickle'))

        # Identify temporal period in graph
        timeNodes = [x for x, dt in G.nodes(data=True) if dt['nodeClass'] == 'time']
        timeNodes = sorted(timeNodes)
        print(timeNodes[0], ' ---> ', timeNodes[-1])

        # Identify unique attribute types
        uniquePlatforms = list(set(nx.get_node_attributes(G, "platform").values()))
        uniqueContentType = list(set(nx.get_node_attributes(G, "contentType").values()))

        # Calculate platform/content type frequency per day
        frequencyDf = pd.Series(timeNodes).to_frame(name='Day')
        frequencyDf[uniquePlatforms] = 0
        frequencyDf[uniqueContentType] = 0
        frequencyDf.set_index('Day', inplace=True)

        idx = pd.MultiIndex.from_product([timeNodes, uniquePlatforms, uniqueContentType], names=['Day', 'Platform', 'Content Type'])
        combinedFrequencyDf = pd.DataFrame(0, idx, ['Count'])
        combinedFrequencyDf.index = combinedFrequencyDf.index.set_levels(combinedFrequencyDf.index.levels[0].date,
                                                                         level=0)

        logging.info('Calculating frequencies from graph')
        for d in timeNodes:
            neighborNodes = list(G.neighbors(d))

            # Count platforms
            neighborPlatforms = [G.nodes[n]['platform'] for n in neighborNodes if G.nodes[n]['nodeClass'] == 'content']
            platformCount = pd.Series(Counter(neighborPlatforms), dtype='object')
            for p, c in platformCount.items():
                frequencyDf.loc[d, p] = c

            # Count content type
            neighborContent = [G.nodes[n]['contentType'] for n in neighborNodes if G.nodes[n]['nodeClass'] == 'content']
            contentCount = pd.Series(Counter(neighborContent))
            for p, c in contentCount.items():
                frequencyDf.loc[d, p] = c

            # Count platform-content type combinations
            neighborCombinations = [[(G.nodes[n]['platform'], G.nodes[n]['contentType'])] for n in neighborNodes
                                    if G.nodes[n]['nodeClass'] == 'content']
            combinationCount = Counter(chain(*neighborCombinations))
            for k, c in combinationCount.items():
                combinedFrequencyDf.loc[(d, k[0], k[1]), 'Count'] = c
            k=1

        # Prepare data for platform plot
        logging.info('>> Platform plot')
        plotDf = None
        for m in uniquePlatforms:
            auxDf = frequencyDf[m].rename('Count').to_frame()
            auxDf['Platform'] = m
            plotDf = auxDf if plotDf is None else pd.concat([plotDf, auxDf], ignore_index=False)

        plotDf = plotDf.reset_index()

        # Make plot
        g = sns.FacetGrid(plotDf, col='Platform', col_wrap=3, height=6, margin_titles=True, xlim=(timeNodes[0], timeNodes[-1]))
        g.map(sns.scatterplot, "Day", "Count", alpha=.6)      # order=['Facebook', 'Google Search', 'YouTube', 'Reddit', 'Twitter']
        plt.savefig(join(config['plotDir'], f'platformDistOverTime.png'), dpi=200)
        plt.close()

        # Prepare data for content type plot
        logging.info('>> Content type plot')
        plotDf = None
        for m in uniqueContentType:
            auxDf = frequencyDf[m].rename('Count').to_frame()
            auxDf['Content Type'] = m
            auxDf['Content Type'] = m
            plotDf = auxDf if plotDf is None else pd.concat([plotDf, auxDf], ignore_index=False)

        plotDf = plotDf.reset_index()

        sns.set(font_scale=3.5)
        HEIGHT = 8.5
        ASPECT = 1.9

        # Make plot
        g = sns.FacetGrid(plotDf, col='Content Type', height=HEIGHT, aspect=ASPECT, margin_titles=True, xlim=(timeNodes[0], timeNodes[-1]), col_wrap=3)
        g.map(sns.scatterplot, "Day", "Count", alpha=.6)
        plt.savefig(join(config['plotDir'], f'ContentDistOverTime.png'), dpi=200)
        plt.close()

        # Make plot for platform/content type
        logging.info('>> Platform/Content type plot')
        combinedFrequencyDf.reset_index(inplace=True)
        combinedFrequencyDf = combinedFrequencyDf[~(combinedFrequencyDf == 0).any(axis=1)]  # Drop zero counts

        print(combinedFrequencyDf.head())
        g = sns.FacetGrid(combinedFrequencyDf, col='Content Type', hue='Platform', height=HEIGHT, aspect=ASPECT, xlim=(timeNodes[0], timeNodes[-1]), col_wrap=3)
        g.map(sns.scatterplot, "Day", "Count", alpha=.6)
        g.add_legend()
        plt.savefig(join(config['plotDir'], f'ContentTypePerPlatform.png'), dpi=200)
        plt.close()

        g = sns.FacetGrid(combinedFrequencyDf, col='Platform', hue='Content Type', height=HEIGHT, aspect=ASPECT, xlim=(timeNodes[0], timeNodes[-1]), col_wrap=3)
        g.map(sns.scatterplot, "Day", "Count", alpha=.6)
        g.add_legend()
        plt.savefig(join(config['plotDir'], f'PlatformPerContentType.png'), dpi=200)
        plt.close()


    except Exception as ex:
        print(traceback.format_exc())
