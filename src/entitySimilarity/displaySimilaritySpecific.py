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
import matplotlib.pyplot as plt
import seaborn as sns
from bson.objectid import ObjectId
import datetime
from pymongo import MongoClient
from libs.mongoLib import updateContentDocs, getFromId, getContentDocsPerPlatform

import libs.visualization as vz
import libs.osLib as ol


def removeDiagonal(A):
    m = A.shape[0]
    strided = np.lib.stride_tricks.as_strided
    s0,s1 = A.strides
    return strided(A.ravel()[1:], shape=(m-1, m), strides=(s0+s1, s1)).reshape(m, -1)


if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir, centralityDir, adjcDir, plotDir = '../../data', '../../data/Centrality', '../../data/adjacencyMatrices', '../../data/plots'
    similarityM = join(adjcDir, '')
    numTopCentral, numNeighbors = 5, 3
    classes = ['spatial', 'tag', 'time']

    classMapping = {
        'time': 'T',
        'content': 'C',
        'tag': 'G',
        'spatial': 'L',
    }
    classMetapath = {
        'T': 'TCT',
        'G': 'GCG',
        'L': 'LCL',
    }

    try:
        # Set up DB
        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionEnt = db['entities']
        collectionSource = db['locations']

        # Load
        centralDf = pd.read_csv(join(centralityDir, 'centralityAsWeGo-degree.csv'), index_col=0)

        # Load similarity matrices (Sparse)
        matrices = {c: ol.loadSparce(join(adjcDir, f'similarity-{"".join(metapath)}.npz')) for c, metapath in classMetapath.items()}
        matrices = {c: m.toarray() for c, m in matrices.items()}
        idToIndexMapping = {c: ol.loadPickle(join(adjcDir, f'idToIndexMapping-{metapath[0]}.pickle')) for c, metapath in classMetapath.items()}
        indexToIdMapping = {}
        for class_, mapping in idToIndexMapping.items():
            indexToIdMapping[class_] = {idx: id for id, idx in mapping.items()}

        # Select most central nodes for each class
        selectedNodes = {}
        for c in classes:
            highestCentralityValues = centralDf[centralDf.nodeClass==c]['degree'].nlargest(n=numTopCentral, keep='first')
            highestCentralityIDs = highestCentralityValues.index.tolist()
            selectedNodes[classMapping[c]] = highestCentralityIDs
            r=1

        # Select the nodes most similiar to them
        for class_, nodes in selectedNodes.items():
            selectedNeighbors = []
            simM = matrices[class_]
            r=1
            try:
                for id in nodes:
                    properId = ObjectId(id) if ObjectId.is_valid(id) else datetime.datetime.strptime(id, "%Y-%m-%d").date()
                    index = idToIndexMapping[class_][properId]
                    topNeighbors = simM[index].argsort()[-numNeighbors:][::-1]
                    selectedNeighbors.append(topNeighbors)
                selectedNodes[class_] += selectedNeighbors
                selectedNodes[class_] = np.unique(selectedNeighbors).tolist()
            except Exception as ex:
                print(traceback.format_exc())
                r=1

        # Get relevant similarity Matrices
        for class_, nodes in selectedNodes.items():
            matrices[class_] = matrices[class_][np.ix_(nodes, nodes)]
            r=1

        # Plot in 2D
        for class_, nodes in selectedNodes.items():
            path = join(plotDir, f'viz-similarity-{class_}.png')
            labelIDS = [indexToIdMapping[class_][n] for n in nodes]
            labels = []
            if class_ != 'T':
                for id in labelIDS:
                    info, matchingColl = getFromId(id, collectionEnt, collectionSource)
                    label = info['label'] if matchingColl is collectionSource else info['mentionForms'][0]
                    labels.append(label)
            else:
                labels = labelIDS

            vz.similarityMatrice(np.round(matrices[class_], decimals=3), labels, labels, path)

    except Exception as ex:
        print(traceback.format_exc())