from os import getcwd
import sys
sys.path.append(getcwd() + '/..')  # Add src/ dir to import path
import traceback
import logging
from os.path import join
from itertools import combinations
from argparse import ArgumentParser

import networkx as nx
import pandas as pd
import numpy as np

from pysclump import SClump
import libs.osLib as ol

if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir, outputDir = '../../data', '../../data/adjacencyMatrices'
    loadNodeMappings, loadAdjacencies = True, False

    previousMethod = 'PathSim'

    parser = ArgumentParser()

    parser.add_argument("-nc", "--numClusters", type=int, default=500)
    parser.add_argument("-ni", "--numIterations", type=int, default=7)
    parser.add_argument("-a", "--alpha", type=float, default=0.5)
    parser.add_argument("-b", "--beta", type=float, default=10)
    parser.add_argument("-g", "--gamma", type=float, default=0.01)
    parser.add_argument("-cu", "--clusterUsing", type=str, default='laplacian')     # similarity
    args = parser.parse_args()

    '''
    Coefficients in the loss function:
        * alpha: for the Frobenius norm of S, 
        * beta: for the L2-norm of lambda, 
        * gamma: for the trace of LS.
    '''

    classMapping = {
        'time': 'T',
        'content': 'C',
        'tag': 'G',
        'spatial': 'L',
    }

    # [['spatial', 'content', 'spatial'], ['spatial', 'content', 'time', 'content', 'spatial'], ['spatial', 'content', 'tag', 'content', 'spatial']]
    # [['tag', 'content', 'tag'], ['tag', 'content', 'time', 'content', 'tag'], ['tag', 'content', 'spatial', 'content', 'tag']]
    # [['time', 'content', 'spatial', 'content', 'time'], ['time', 'content', 'tag', 'content', 'time']]

    metapathsToInclude = [['tag', 'content', 'tag'], ['tag', 'content', 'time', 'content', 'tag'], ['tag', 'content', 'spatial', 'content', 'tag']]
    metapathsToInclude = [[classMapping[t] for t in metapath] for metapath in metapathsToInclude]

    try:
        assert(len([m for m in metapathsToInclude if m[0]!=m[-1]]) == 0)    # Assert all metapaths are valid in itself
        assert(len([it for it in range(1, len(metapathsToInclude)) if metapathsToInclude[it][0] != metapathsToInclude[it-1][0]]) == 0)   # Assert all metpatahs are valid between themselves
        targetClass = metapathsToInclude[0][0]

        # Load similarity matrices (+ convert from sparse to numpy)
        similarityMatrices = {
            ''.join(metapath): ol.loadSparce(join(outputDir, f'{previousMethod}-similarity-{"".join(metapath)}.npz')).toarray()
            for metapath in metapathsToInclude
        }

        # Init logger
        import wandb
        wandb.init(project="sclump", entity="jointleman")
        wandb.config.update(args)
        outputDir = wandb.run.dir

        # Create SClump instance
        sclump = SClump(similarityMatrices, num_clusters=args.numClusters)

        # Run the algorithm
        print('Running Algo')
        labels, learnedSimMatrix, metapathWeights, finalLoss = sclump.run(verbose=False, cluster_using=args.clusterUsing,
                                  alpha=args.alpha, beta=args.beta, gamma=args.gamma,
                                  num_iterations=args.numIterations)

        # Save results
        print('Saving results')
        ol.saveNumpy(learnedSimMatrix, join(outputDir, f'SClump-similarity-{targetClass}.npy'))
        ol.savePickle(metapathsToInclude, join(outputDir, f'SClump-metapaths-{targetClass}.pkl'))
        ol.savePickle(labels, join(outputDir, f'SClump-labels-{targetClass}.pkl'))
        ol.savePickle(metapathWeights, join(outputDir, f'SClump-metapathWeights-{targetClass}.pkl'))
        ol.savePickle(metapathWeights, join(outputDir, f'SClump-metapathWeights-{targetClass}.pkl'))

    except Exception as ex:
        print(traceback.format_exc())