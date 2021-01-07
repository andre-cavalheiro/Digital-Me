import traceback
import logging

import numpy as np
import pandas as pd
import networkx as nx
import powerlaw
import os
from scipy.sparse import csr_matrix


def calculateCentrality(G, nodeDf, measurements, saveAsWeGo=True, saveDir=None):

    functionMapping = {
        'degree': nx.degree_centrality,
        'betweenness': nx.betweenness_centrality,
        'closeness': nx.closeness_centrality,
        'closeness': nx.eigenvector_centrality,
        'katz': nx.katz_centrality,
    }

    for m in measurements:
        logging.info(f'Calculating - {m}')
        centralityValues = functionMapping[m](G)
        tempDf = pd.DataFrame.from_dict(centralityValues, orient='index', columns=[m])
        nodeDf = pd.merge(nodeDf, tempDf, left_index=True, right_index=True)

        if saveAsWeGo is True:
            saveDir = os.getcwd() if saveDir is None else saveDir
            nodeDf.to_csv(os.path.join(saveDir, f'centralityAsWeGo-{m}.csv'))

    return nodeDf


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


def generalNetworkAnalysis(degreeValues):
    '''
    :param degreeValues:
    :return:
    '''
    alpha, sigma = fitPowerLaw(degreeValues)

    avgDegree, maxDegree, minDegree, fstP, sndP, trdP = getStatistics(degreeValues)

    return avgDegree, maxDegree, minDegree, fstP, sndP, trdP, alpha, sigma


def getStatistics(arrayOfVals):
    '''
    :param arrayOfVals: List of degree values.
    :return:
    '''

    avg = sum(arrayOfVals) / len(arrayOfVals)
    max_ = max(arrayOfVals)
    min_ = min(arrayOfVals)

    fstP = np.percentile(arrayOfVals, 25)
    sndP = np.percentile(arrayOfVals, 50)
    trdP = np.percentile(arrayOfVals, 75)

    return avg, max_, min_, fstP, sndP, trdP


def fitPowerLaw(degrees):
    fit = powerlaw.Fit(degrees)
    alpha, sigma = fit.power_law.alpha, fit.power_law.sigma
    print('alpha= ', fit.power_law.alpha, '  sigma= ', fit.power_law.sigma)
    return alpha, sigma
