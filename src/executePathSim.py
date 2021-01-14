import traceback
import logging
from os.path import join
from datetime import date, timedelta, datetime
from itertools import combinations

import networkx as nx
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pymongo import MongoClient

from pysclump import PathSim
import libs.networkAnalysis as na
import libs.visualization as vz
from libs.mongoLib import updateContentDocs

if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir = '../data/'

    try:
        # Load graph from OS
        G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))

        # ...
        classPerNode = nx.get_node_attributes(G, "nodeClass")
        nodesPerClass = {}
        for key, val in classPerNode.items():
            nodesPerClass[val] = nodesPerClass.get(val, []) + [key]

        # ...
        uniqueClasses = set(classPerNode.values())
        classCombinations = list(combinations(uniqueClasses, 2))

        # Get node list to avoid dictionaries' underterministic stuff
        nodes = list(G.nodes())

        # Get incidence matrices between any node types
        incidenceMatrix = nx.incidence_matrix(G, nodelist=nodes)
        incidenceMatrices = {}

        for (headClass, tailClass) in classCombinations[2:]:    # FIXME

            incidenceM = incidenceMatrix.copy()

            indexesClassHead = [nodes.index(n) for n in nodesPerClass[headClass]]
            indexesClassTail = [nodes.index(n) for n in nodesPerClass[tailClass]]

            incidenceM = incidenceM.tocsr()[indexesClassHead]
            incidenceM = incidenceM.tocsr()[:, indexesClassTail]
            incidenceM = incidenceM.A  # from sparce to numpy

            logging.info(f'Incidence shape: f{incidenceM.shape}, supposed to be {len(nodesPerClass[headClass]), len(nodesPerClass[tailClass])}')

            incidenceMatrices[(headClass, tailClass)] = incidenceM
            np.save(f'incidence_{headClass}-{tailClass}.npy', incidenceM)

    except Exception as ex:
        print(traceback.format_exc())

    # Example
    '''
    from pysclump import PathSim
    import numpy as np
    # key=classes, list of IDs
    type_lists = {
        'A': ['Mike', 'Jim', 'Mary', 'Bob', 'Ann'],
        'C': ['SIGMOD', 'VLDB', 'ICDE', 'KDD'],
        'V': ['Pasadena', 'Guwahati', 'Bangalore']
    }

    # Incidence matrix between classes - https://networkx.github.io/documentation/networkx-1.10/reference/generated/networkx.linalg.graphmatrix.incidence_matrix.html
    incidence_matrices = {
        'AC': np.array([[2, 1, 0, 0], [50, 20, 0, 0], [2, 0, 1, 0], [2, 1, 0, 0], [0, 0, 1, 1]]),
        'VC': np.array([[3, 1, 1, 1], [1, 0, 0, 0], [2, 1, 0, 1]])
    }

    # Create PathSim instance.
    ps = PathSim(type_lists, incidence_matrices)

    # Get the similarity between two authors (indicated by type 'A').
    ps.pathsim('Mike', 'Jim', metapath='ACA')

    # Get the similarity matrix M for the metapath.
    ps.compute_similarity_matrix(metapath='ACVCA')
    '''