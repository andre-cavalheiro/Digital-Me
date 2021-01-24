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
        metapathsToInclude = [['tag', 'content', 'tag'], ['tag', 'content', 'day', 'content', 'tag']]
        metapathsToInclude = [[classMapping[t] for t in metapath] for metapath in metapathsToInclude]

        # Load similarity matrices
        similarityMatrices = {
            ''.join(metapath): ol.loadSparce(join(outputDir, f'similarity-{"".join(metapath)}.npz'))
            for metapath in metapathsToInclude
        }

        # Create SClump instance.
        sclump = SClump(similarityMatrices, num_clusters=numClusters)

        # Run the algorithm!
        labels, learnedSimMatrix, metapathWeights = sclump.run()

        # Save results
        ol.saveSparce(learnedSimMatrix, join(outputDir, f'SClump-similarity.npz'))
        ol.savePickle(labels, join(outputDir, f'SClump-labels.npz'))
        ol.savePickle(metapathWeights, join(outputDir, f'SClump-metapathWeights.npz'))
        ol.savePickle(metapathsToInclude, join(outputDir, f'SClump-metapaths.npz'))



    except Exception as ex:
        print(traceback.format_exc())