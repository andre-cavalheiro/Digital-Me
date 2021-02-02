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

if __name__ == '__main__':

    root = logging.getLogger()

    baseDir = '../../data/'

    try:
        # Load graph from OS
        G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))

        simDict = nx.simrank_similarity(G)      # Calculate one by one? Once per node should take quite a while but should facilitate memory wise.

        savePickle(simDict, max_iterations=10)

        # TODO - must convert this into a matrice

    except Exception as ex:
        print(traceback.format_exc())