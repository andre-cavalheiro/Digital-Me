import traceback
import logging
from os.path import join
from datetime import date, timedelta, datetime

import networkx as nx
import pandas as pd
import numpy as np
import wandb
import matplotlib.pyplot as plt


def calculateCentrality(G, df, measurements):

    functionMapping = {
        'degree': nx.degree_centrality,
        'betweenness': nx.betweenness_centrality,
        'closeness': nx.closeness_centrality,
        'eigenvector': nx.eigenvector_centrality,
        'katz': nx.katz_centrality,
    }

    for m in measurements:
        logging.info(f'Calculating - {m}')
        centralityValues = functionMapping[m](G)
        tempDf = pd.DataFrame.from_dict(centralityValues, orient='index', columns=[m])
        df = pd.merge(df, tempDf, left_index=True, right_index=True)

    return df

def drawBoxPlots(data, xlabels, savingPath, **kargs):
    # todo - this may help for w&B: https://github.com/wandb/examples/blob/master/matplotlib/mpl.py
    positions = np.arange(len(data)) + 1

    fig = plt.figure(figsize=(10, 10))
    ax = plt.axes()

    # plt.xticks(positions, xlabels)
    bp = ax.boxplot(data, positions=positions, showmeans=True, **kargs)

    ax.set_xticklabels(xlabels, rotation=45, ha='right')
    plt.savefig(savingPath)
    # plt.show()
    plt.close()
    return plt


def getOnlyConnectedGraph(g, prints=True):
    numComponents = nx.number_connected_components(g)
    giantComponentNodes = max(nx.connected_components(g), key=len)
    giantComponent = g.subgraph(giantComponentNodes)

    if prints:
        nOriginal, eOriginal = g.number_of_nodes(), g.number_of_edges()
        nKept, eKept = giantComponent.number_of_nodes(), giantComponent.number_of_edges()
        nDropped, eDropped = g.number_of_nodes() - nKept, g.number_of_edges() - eKept
        print(f'\t - The network has {numComponents} connected components (N: {nOriginal}\t E: {eOriginal}\t)\n '
              f'\t - Returning only the biggest (N:{nKept}\t E: {eKept})\n'
              f'\t - Dropped a total of {nDropped} nodes and {eDropped} edges\n')

    return giantComponent



if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir = '../data/'

    measurements = ['degree']

    try:
        # Load graph from OS
        G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))
        G = getOnlyConnectedGraph(G)

        # Identify node classes
        df = pd.DataFrame.from_dict({n: G.nodes[n]['nodeClass'] for n in G.nodes()}, orient='index', columns=['nodeClass'])
        nodeClasses = df.nodeClass.unique().tolist()

        # Identify platforms for content nodes
        auxDf = pd.DataFrame.from_dict({n: G.nodes[n]['platform'] if 'platform' in G.nodes[n].keys() else None
                                     for n in G.nodes()}, orient='index', columns=['platform'])
        platforms = auxDf.platform.unique().tolist()
        platforms.remove(None)
        df = pd.merge(df, auxDf, left_index=True, right_index=True)

        # Calculate centrality values
        df = calculateCentrality(G, df, measurements)

        #  Draw box plots per class
        for m in measurements:
            data = []
            for c in nodeClasses:
                print(f'{m}/{c}')
                data.append(df[df.nodeClass == c][m].tolist())
            drawBoxPlots(data, nodeClasses, join(baseDir, f'{m}-class.png'), showfliers=False, meanline=True)

        # Draw box plots per platform
        for m in measurements:
            data = []
            for p in platforms:
                print(f'{m}/{p}')
                dt = df[df.platform == p][m].tolist()
                data.append(dt)
            drawBoxPlots(data, platforms, join(baseDir, f'{m}-platform.png'), showfliers=False, meanline=True)


    except Exception as ex:
        print(traceback.format_exc())
