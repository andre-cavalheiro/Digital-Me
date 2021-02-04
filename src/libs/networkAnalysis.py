import traceback
import logging
import os
from functools import reduce

import numpy as np
import pandas as pd
import powerlaw
import networkx as nx
from scipy.sparse import csr_matrix
from multiprocessing import Pool
import itertools


def calculateCentrality(G, nodeDf, measurements, saveAsWeGo=True, saveDir=None):

    functionMapping = {
        'degree': nx.degree_centrality,
        'betweenness': nx.betweenness_centrality,
        'betweennessParallel': betweenness_centrality_parallel,
        'closeness': nx.closeness_centrality,
        'eigenvector': nx.eigenvector_centrality,
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
        print(f'\t - The network has {numComponents} connected components (N: {nOriginal}\t E: {eOriginal})\n '
              f'\t - Returning only the biggest (N:{nKept}\t E: {eKept})\n'
              f'\t - Dropped a total of {nDropped} nodes and {eDropped} edges\n')

    return giantComponent


def adjacencyBetweenTypes(G,  nodesPerClass, classA, classB):
    '''

    :param G:
    :param nodesPerClass:
    :param classA:
    :param classB:
    :return: scipy_sparse_matrix
    '''
    nodesClassA, nodesClassB = nodesPerClass[classA], nodesPerClass[classB]
    validNodes = nodesClassA + nodesClassB
    adjacencyM = nx.to_scipy_sparse_matrix(G, nodelist=validNodes)
    adjacencyM = adjacencyM[:len(nodesClassA), len(nodesClassA):]
    assert(adjacencyM.shape == (len(nodesClassA), len(nodesClassB)))
    return adjacencyM


def generalNetworkStats(degreeValues):
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


def betweenness_centrality_parallel(G, processes=None):
    """Parallel betweenness centrality  function"""

    def chunks(l, n):
        """Divide a list of nodes `l` in `n` chunks"""
        l_c = iter(l)
        while 1:
            x = tuple(itertools.islice(l_c, n))
            if not x:
                return
            yield x

    p = Pool(processes=processes)
    node_divisor = len(p._pool) * 4
    node_chunks = list(chunks(G.nodes(), int(G.order() / node_divisor)))
    num_chunks = len(node_chunks)
    bt_sc = p.starmap(
        nx.betweenness_centrality_subset,
        zip(
            [G] * num_chunks,
            node_chunks,
            [list(G)] * num_chunks,
            [True] * num_chunks,
            [None] * num_chunks,
        ),
    )

    # Reduce the partial solutions
    bt_c = bt_sc[0]
    for bt in bt_sc[1:]:
        for n in bt:
            bt_c[n] += bt[n]
    return bt_c
