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

import libs.osLib as ol


def removeDiagonal(A):
    m = A.shape[0]
    strided = np.lib.stride_tricks.as_strided
    s0,s1 = A.strides
    return strided(A.ravel()[1:], shape=(m-1, m), strides=(s0+s1, s1)).reshape(m, -1)


if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir, outputDir = '../../data/adjacencyMatrices', '../../data/plots'
    loadNodeMappings, loadAdjacencies = True, False

    numClusters = 2

    classMapping = {
        'time': 'T',
        'content': 'C',
        'tag': 'G',
        'location': 'L',
    }

    try:
        # metapaths = [['time', 'content', 'time'], ['tag', 'content', 'tag'], ['location', 'content', 'location'] ] # ['time', 'content', 'time']    #     #
        metapaths = [['time', 'content', 'time']]
        metapaths = [[classMapping[t] for t in metapath] for metapath in metapaths]

        for metapath in metapaths:
            nodeMapping = ol.loadPickle(join(baseDir, f'nodeMapping.pickle'))

            # PathSim load
            similarityM = ol.loadSparce(join(baseDir, f'similarity-{"".join(metapath)}.npz')).toarray()

            # Sclump load
            # similarityM = ol.loadNumpy(join(baseDir, f'SClump-similarity.npy'))

            similarityM = removeDiagonal(similarityM)   # Carefull here - we're removing the relation with itself but breaking the mapping from nodeMapping

            # Remove all zeros
            print(f'Orig shape: {similarityM.shape}')
            similarityM = similarityM[~np.all(similarityM == 0, axis=1)]
            similarityM = similarityM[:, ~np.all(similarityM == 0, axis=0)]
            print(f'Without zeros shape: {similarityM.shape}')

            # Plot simple value histogram
            flattenSim = pd.Series(similarityM.flatten())
            g = sns.distplot(flattenSim, kde=False, bins=10)
            g.set_yscale('log')
            plt.savefig(join(outputDir, f'similarityValueDistribution-{"".join(metapath)}.png'))
            plt.title('Value count in Similarity Matrix')
            print(similarityM.max())

            # Count non-zeros per row
            rowCountNonZero = np.count_nonzero(similarityM, axis=1)

            # Count max value per row
            rowCountMax = np.amax(similarityM, 1)

            # Count min value (that's not a zero) per row
            rowCountMinNonZero = np.where(similarityM > 0, similarityM, similarityM.max()).min(1)

            # Count mean value (that's not a zero) per row
            rowCountMeanNonZero = np.true_divide(similarityM.sum(1), (similarityM!=0).sum(1))

            plotDf = None
            for k, x in {
                'Non zeros per row': rowCountNonZero,
                'Max per row': rowCountMax,
                'Mean per row (no zeros)': rowCountMeanNonZero,
                'Min per row (no zeros)': rowCountMinNonZero,
            }.items():
                auxDf = pd.Series(x, name='Count').to_frame()
                auxDf['Measure'] = k
                plotDf = auxDf if plotDf is None else pd.concat([plotDf, auxDf], ignore_index=False)

            # Make boxplot
            fig, ax = plt.subplots(figsize=(15, 15))
            g = sns.boxplot(ax=ax, data=plotDf, x='Measure', y='Count', palette="Set2", showfliers=True, showmeans=True)
            g.set_yscale('log')
            g.set_yticklabels(g.get_yticks(), size=16)
            # g.set_xticklabels(g.get_xticks(), size=16)
            plt.savefig(join(outputDir, f'statsPerRow-log-{"".join(metapath)}.png'))
            plt.close()

            # Make boxplot
            fig, ax = plt.subplots(figsize=(15, 15))
            g = sns.boxplot(ax=ax, data=plotDf, x='Measure', y='Count', palette="Set2", showfliers=False, showmeans=True)
            g.set_yticklabels(g.get_yticks(), size=16)
            # g.set_xticklabels(g.get_xticks(), size=16)
            plt.savefig(join(outputDir, f'statsPerRow-{"".join(metapath)}.png'))
            plt.close()

            # Make violin plots
            fig = plt.figure(figsize=(12, 12))
            gs = fig.add_gridspec(3, 2)

            ax = fig.add_subplot(gs[0, 0])
            sns.violinplot(data=similarityM.flatten())
            ax.set_xlabel("Similarity as is")

            ax = fig.add_subplot(gs[0, 1])
            sns.violinplot(data=rowCountNonZero)
            ax.set_xlabel("Non zeros per row")

            ax = fig.add_subplot(gs[1, 0])
            sns.violinplot(rowCountMeanNonZero)
            ax.set_xlabel("Mean per row (no zeros)")

            ax = fig.add_subplot(gs[1, 1])
            sns.violinplot(rowCountMinNonZero)
            ax.set_xlabel("Min per row (no zeros)")

            ax = fig.add_subplot(gs[2, 0])
            sns.violinplot(data=rowCountMax)
            ax.set_xlabel("Max per row")

            fig.tight_layout()
            plt.savefig(join(outputDir, f'statsViolinPerRow-{"".join(metapath)}.png'))
            plt.close()

            # Plot as matrix
            """
            fig = plt.figure(figsize=(15, 15))
            ax = plt.axes()
            plt.spy(similarityM, precision=0.1, marker='.', markersize=0.05)
            plt.savefig(join(outputDir, f'similarityMatrixPlot-{"".join(metapath)}.png'))
            plt.close()
            """



        # Select top k most similiar or wtv

        # Pick their similarity vectors

        # Plot them

    except Exception as ex:
        print(traceback.format_exc())