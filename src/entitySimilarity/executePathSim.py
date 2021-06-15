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
from scipy import sparse

from pysclump import PathSim
from libs.networkAnalysis import adjacencyBetweenTypes
import libs.osLib as ol

if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir, outputDir = '../../data', '../../data/adjacencyMatrices'
    loadIDtoTempIndexs, loadAdjacencies = True, True

    classMapping = {
        'time': 'T',
        'content': 'C',
        'tag': 'G',
        'spatial': 'L',
    }
    metapaths = [
     ['spatial', 'content', 'spatial'], ['spatial', 'content', 'time', 'content', 'spatial'], ['spatial', 'content', 'tag', 'content', 'spatial'],
     ['tag', 'content', 'tag'], ['tag', 'content', 'time', 'content', 'tag'], ['tag', 'content', 'spatial', 'content', 'tag'],
     ['time', 'content', 'time'], ['time', 'content', 'spatial', 'content', 'time'], ['time', 'content', 'tag', 'content', 'time']
    ]
    metapaths = [[classMapping[t] for t in metapath] for metapath in metapaths]

    try:
        # Load graph from OS
        G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))

        # Get node list per class type
        classPerID = nx.get_node_attributes(G, "nodeClass")
        nodesPerClass = {}
        for id, class_ in classPerID.items():
            classDim = classMapping[class_]
            nodesPerClass[classDim] = nodesPerClass.get(classDim, []) + [id]  # Error, spatial

        # Calculate similarity matrice for each metapath
        for metapath in metapaths:
            print(f'=============== {metapath} ===============')
            targetClass = metapath[0]

            # Get ID to Index type for specific class
            if loadIDtoTempIndexs is True:
                IdToIndex = ol.loadPickle(join(outputDir, f'PathSim-IdToIndexMapping-{targetClass}.pickle'))
            else:
                IdToIndex = {id: idx for idx, id in enumerate(nodesPerClass[targetClass])}
                ol.savePickle(IdToIndex, join(outputDir, f'PathSim-IdToIndexMapping-{targetClass}.pickle'))

            # Identify adjacency matrices necessary for specific meta-path
            classCombinations = [(metapath[i], metapath[i + 1]) for i in range(len(metapath) - 1)]

            # Get adjacency matrices
            if loadAdjacencies is True:
                adjacencies = {f'{classA+classB}': ol.loadSparce(join(outputDir, f'adjacency-{classA+classB}.npz'))
                               for classA, classB in classCombinations}
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
            ol.saveSparce(similarityM, join(outputDir, f'PathSim-similarity-{"".join(metapath)}.npz'))


    except Exception as ex:
        print(traceback.format_exc())