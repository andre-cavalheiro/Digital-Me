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

    allCombinations = False  # Takes a lot of memory

    try:
        # Load graph from OS
        G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))

        if allCombinations is True:
            simDict = nx.simrank_similarity(G, max_iterations=10)      # Calculate one by one? Once per node should take quite a while but should facilitate memory wise.
            savePickle(simDict, 'SimRank-similarity')
        else:
            print('Loading selected nodes')
            indexesToCalcPerClass = ol.loadPickle(join(outputDir, 'selectedNodes'))
            for class_, indexesToCalc in indexesToCalcPerClass.items():
                print(class_)
                results = {}
                for source in indexesToCalc:
                    targets = [n for n in indexesToCalc if n != source]
                    for t in targets:
                        print(f'-> {t}')
                        sim = nx.simrank_similarity(G, source=source, target=t, max_iterations=3)
                        print(sim)
                        results[source][t] = sim
                savePickle(results, f'SimRank-similarity-specific-{class_}')

    except Exception as ex:
        print(traceback.format_exc())