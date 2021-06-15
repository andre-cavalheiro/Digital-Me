from os import getcwd
import sys

sys.path.append(getcwd() + '/..')  # Add src/ dir to import path
import traceback
import logging
from os.path import join
from itertools import combinations
import datetime

import networkx as nx
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from bson.objectid import ObjectId
from pymongo import MongoClient
from libs.mongoLib import updateContentDocs, getFromId, getContentDocsPerPlatform

import libs.visualization as vz
import libs.osLib as ol


if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir, centralityDir, adjcDir, plotDir = '../../data', '../../data/Centrality', '../../data/adjacencyMatrices', '../../data/Plots'
    centralityPath = 'centralityAsWeGo-degree.csv'

    numTopCentral, numNeighbors = 3, 3
    classMapping = {
        'T': 'time',
        'C': 'content',
        'G': 'tag',
        'L': 'spatial',
    }

    # This controls what's actually done
    classMatricePaths = {
        # 'T': [join(adjcDir, f'PathSim-similarity-TCT.npz')],
        # 'G': [join(adjcDir, f'PathSim-similarity-GCG.npz')],
        'T': [join(adjcDir, f'PathSim-similarity-TCT.npz'), join(adjcDir, f'PathSim-similarity-TCLCT.npz'), join(adjcDir, f'PathSim-similarity-TCGCT.npz')],
        'G': [join(adjcDir, f'PathSim-similarity-GCG.npz'), join(adjcDir, f'PathSim-similarity-GCTCG.npz'), join(adjcDir, f'PathSim-similarity-GCLCG.npz')],
        'L': [join(adjcDir, f'PathSim-similarity-LCL.npz'), join(adjcDir, f'PathSim-similarity-LCGCL.npz'), join(adjcDir, f'PathSim-similarity-LCTCL.npz')],
    }

    classMatriceNames = {
        'T': ['PathSim(TCT)', 'PathSim(TCLCT)', 'PathSim(TCGCT)'],
        'G': ['PathSim(GCG)', 'PathSim(GCTCG)', 'PathSim(GCLCG)'],
        'L': ['PathSim(LCL)', 'PathSim(LCGCL)', 'PathSim(LCTCL)'],
    }

    assert(len([c for c, ms in classMatricePaths.items() if len(ms)!=len(classMatriceNames[c])])==0)    # Assert matrice names are all properly set

    try:
        # Set up DB (only used for plot labels)
        '''
        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionEnt = db['entities']
        collectionSource = db['locations']
        '''

        wordCorpusList = ['Ola Ola Ola Tudo Tudo Bem Bem Fui Fui Ali Ali Ali Ali Ali Ali Ali Passear'
                          'Passear Passear E E E VI Duas Duas Duas Duas Duas Duas Duas Pombas Pombas Pombas',
                          'Ola Ola Ola Tudo Tudo Bem Bem Fui Fui Ali Ali Ali Ali Ali Ali Ali Passear'
                          'Passear Passear E E E VI Duas Duas Duas Duas Duas Duas Duas Pombas Pombas Pombas']

        vz.smallMultipleWordCloud(wordCorpusList)

        """
        # Load centrality DF
        centralDf = pd.read_csv(join(centralityDir, centralityPath), index_col=0)

        # Load similarity matrices (+ convert from sparse to numpy)
        matrices = {c: [ol.loadSparce(p).toarray() for p in pathList]
                    for c, pathList in classMatricePaths.items()}
        idToIndexMapping = {c: ol.loadPickle(join(adjcDir, f'PathSim-idToIndexMapping-{c}.pickle'))
                            for c in ['T', 'G', 'L']}
        indexToIdMapping = {class_: {idx: id for id, idx in mapping.items()} for class_, mapping in idToIndexMapping.items()}


        # --------------- Selection of nodes to display ---------------

        # Select most central nodes for each class
        selectedNodes = {}
        for c in classMatricePaths.keys():
            highestCentralityValues = centralDf[centralDf.nodeClass == classMapping[c]]['degree'].nlargest(n=numTopCentral, keep='first')
            highestCentralityIDs = highestCentralityValues.index.tolist()
            selectedNodes[c] = highestCentralityIDs

        # Select the nodes most similiar to them (for each similairty matrice)
        for class_, nodes in selectedNodes.items():
            selectedNeighbors = []
            for simM in matrices[class_]:
                try:
                    for id in nodes:
                        properId = ObjectId(id) if ObjectId.is_valid(id) else datetime.datetime.strptime(id, "%Y-%m-%d").date()
                        index = idToIndexMapping[class_][properId]

                        topNeighbors = simM[index].argsort()[-numNeighbors:][::-1]

                        topNeighbors = [indexToIdMapping[class_][n] for n in topNeighbors]
                        selectedNeighbors += topNeighbors
                except Exception as ex:
                    print(traceback.format_exc())

            selectedNodes[class_] += selectedNeighbors
            selectedNodes[class_] = np.unique(selectedNeighbors).tolist()

        # Not mandatory to save - only did it for another experiment
        ol.savePickle(selectedNodes, join(adjcDir, 'selectedNodes'))

        # Convert from ID to Index
        selectedNodes = {class_: [idToIndexMapping[class_][n] for n in nodes] for class_, nodes in selectedNodes.items()}

        # ------------------------------

        # Get only the relevant parts for each similarity Matrices
        for class_, nodes in selectedNodes.items():
            for it in range(len(matrices[class_])):
                matrices[class_][it] = matrices[class_][it][np.ix_(nodes, nodes)]

        # Plot in 2D
        for class_, nodes in selectedNodes.items():
            # Get proper entity labels from database
            labels = []
            labelIDS = [indexToIdMapping[class_][n] for n in nodes]
            if class_ != 'T':
                for id in labelIDS:
                    info, matchingColl = getFromId(id, collectionEnt, collectionSource)
                    label = info['label'] if matchingColl is collectionSource else info['mentionForms'][0]
                    labels.append(label)
            else:
                labels = labelIDS

            # Plot matrice heatmaps
            path = join(plotDir, f'viz-similarity-{class_}.png')
            print(path)
            titles = []
            matrices[class_] = [np.round(m, decimals=3) for m in matrices[class_]]  # Use only 3 decimal points
            vz.matrixHeatMap(matrices[class_], labels, labels, path, classMatriceNames[class_])
        """

    except Exception as ex:
        print(traceback.format_exc())