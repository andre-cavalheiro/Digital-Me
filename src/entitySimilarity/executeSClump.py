from os import getcwd
import sys

sys.path.append(getcwd() + '/..')  # Add src/ dir to import path
import traceback
import logging
from os.path import join
from itertools import combinations

import networkx as nx
import pandas as pd
import numpy as np

from pysclumpOriginal import SClump
import libs.osLib as ol

if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir, outputDir = '../../data', '../../data/adjacencyMatrices'
    loadNodeMappings, loadAdjacencies = True, False

    numClusters = 2

    classMapping = {
        'time': 'T',
        'content': 'C',
        'tag': 'G',
        'location': 'L',
    }

    try:
        metapathsToInclude = [['tag', 'content', 'tag']]  # [['time', 'content', 'time']]
        metapathsToInclude = [[classMapping[t] for t in metapath] for metapath in metapathsToInclude]

        # Load similarity matrices
        similarityMatrices = {
            ''.join(metapath): ol.loadSparce(join(outputDir, f'similarity-{"".join(metapath)}.npz')).toarray()
            for metapath in metapathsToInclude
        }

        # Create SClump instance.
        sclump = SClump(similarityMatrices, num_clusters=numClusters)

        # Run the algorithm!
        labels, learnedSimMatrix, metapathWeights = sclump.run(verbose=True, cluster_using='similarity', num_iterations=3, alpha=0.5, beta=10, gamma=0.01)    # cluster_using='laplacian'

        # Save results
        ol.saveNumpy(learnedSimMatrix, join(outputDir, f'SClump-similarity.npy'))
        ol.savePickle(metapathsToInclude, join(outputDir, f'SClump-metapaths.pkl'))
        ol.savePickle(labels, join(outputDir, f'SClump-labels.pkl'))
        ol.savePickle(metapathWeights, join(outputDir, f'SClump-metapathWeights.pkl'))

    except Exception as ex:
        print(traceback.format_exc())