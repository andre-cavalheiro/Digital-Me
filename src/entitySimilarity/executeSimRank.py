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

        sim = nx.simrank_similarity(G)

        savePickle(sim, max_iterations=10)

    except Exception as ex:
        print(traceback.format_exc())