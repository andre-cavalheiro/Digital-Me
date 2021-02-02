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

from pysclumpOriginal import PathSim
from libs.networkAnalysis import adjacencyBetweenTypes
import libs.osLib as ol

if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir, outputDir = '../../data', '../../data/adjacencyMatrices'
    loadNodeMappings, loadAdjacencies = True, False

    classMapping = {
        'time': 'T',
        'content': 'C',
        'tag': 'G',
        'location': 'L',
    }

    try:
        from scipy import sparse

        metapath = ['time', 'content', 'time']    # ['location', 'content', 'location']     # ['tag', 'content', 'tag']
        metapath = [classMapping[t] for t in metapath]

        # Load graph from OS
        G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))

        # Change IDs from MongoDB objects to integers for a more convenient slicing
        if loadNodeMappings is True:
            nodeMapping = ol.loadPickle(join(outputDir, f'nodeMapping.pickle'))
        else:
            it, nodeMapping = 0, {}
            for id in G.nodes:
                nodeMapping[id] = it
                it += 1
            ol.savePickle(nodeMapping, join(outputDir, f'nodeMapping.pickle'))
        G = nx.relabel_nodes(G, nodeMapping)

        # Get node list per class type
        classPerNode = nx.get_node_attributes(G, "nodeClass")
        nodesPerClass = {}
        for key, val in classPerNode.items():
            nodesPerClass[classMapping[val]] = nodesPerClass.get(classMapping[val], []) + [key]

        # Identify adjacency matrices necessary for specific metapath
        classCombinations = [(metapath[i], metapath[i + 1]) for i in range(len(metapath) - 1)]

        # Get adjacency matrices
        if loadAdjacencies is True:
            adjacencies = [ol.loadSparce(join(outputDir, f'{c[0]+c[1]}.npz')) for c in classCombinations]
        else:
            # Get necessary adjacency matrices
            logging.info(f'Graph has {len(G.nodes)} nodes')
            adjacencies = {f'{classA+classB}': adjacencyBetweenTypes(G, nodesPerClass, classA, classB)
                           for classA, classB in classCombinations}
            logging.info(f'Adjacency matrices calculated')

            # Save Adjacency Matrices
            for comb, M in adjacencies.items():
                ol.saveSparce(M, join(outputDir, f'{comb}.npz'))

        # Create PathSim instance.
        logging.info(f'Initiating PathSimInstance')
        ps = PathSim(nodesPerClass, adjacencies)

        # Get the similarity matrix M for the metapath.
        logging.info(f'Computing similarity Matrix')
        similarityM = ps.compute_similarity_matrix(metapath=''.join(metapath))
        logging.info(f'All done')
        ol.saveSparce(similarityM, join(outputDir, f'similarity-{"".join(metapath)}.npz'))


    except Exception as ex:
        print(traceback.format_exc())