from os import getcwd
import sys
sys.path.append(getcwd() + '/..')   # Add src/ dir to import path
import traceback
import logging
from os.path import join
from itertools import combinations

import networkx as nx
import pandas as pd
import numpy as np

from libs.osLib import savePickle
import libs.osLib as ol

if __name__ == '__main__':

    root = logging.getLogger()

    baseDir, outputDir = '../../data', '../../data/adjacencyMatrices'

    allCombinations = True  # Takes a lot of memory
    indexesToCalc = []

    try:
        # Load graph from OS
        G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))
        nodeMapping = ol.loadPickle(join(outputDir, f'nodeMapping.pickle'))

        if allCombinations is True:
            simDict = nx.simrank_similarity(G)      # Calculate one by one? Once per node should take quite a while but should facilitate memory wise.
            savePickle(simDict, max_iterations=10)
        else:
            results = {}
            for i in indexesToCalc:
                simDict = nx.simrank_similarity(G, source=i)
                results[i] = simDict
            savePickle(results, max_iterations=10)




    except Exception as ex:
        print(traceback.format_exc())