import traceback
import logging
from os.path import join
from datetime import date, timedelta, datetime

import networkx as nx
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pymongo import MongoClient

import libs.networkAnalysis as na
from libs.mongoLib import updateContentDocs
import libs.visualization as vz

if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    baseDir = '../data/'

    measurements = ['degree', 'betweenness']    # 'closeness', 'katz'
    saveCentralityToOS, saveCentralityToDB = True, False

    try:
        # Load graph from OS
        G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))

        # Power law plot
        degrees = [degree for id, degree in G.degree()]
        vz.degreeDistWithPowerLaw(degrees, join(baseDir, 'degreeDist.png'))

        # Identify node classes
        df = pd.DataFrame.from_dict({n: G.nodes[n]['nodeClass'] for n in G.nodes()}, orient='index', columns=['nodeClass'])
        nodeClasses = df.nodeClass.unique().tolist()

        # Identify platforms for content nodes
        auxDf = pd.DataFrame.from_dict({n: G.nodes[n]['platform'] if 'platform' in G.nodes[n].keys() else None
                                     for n in G.nodes()}, orient='index', columns=['platform'])
        platforms = auxDf.platform.unique().tolist()
        platforms.remove(None)
        df = pd.merge(df, auxDf, left_index=True, right_index=True)

        # Calculate centrality values
        df = na.calculateCentrality(G, df, measurements, saveAsWeGo=True, saveDir=baseDir)
        
        # Centrality distributions per class
        # Inspired by: https://stackoverflow.com/questions/42004381/box-plot-of-a-many-pandas-dataframes
        plotDf = None
        for m in measurements:
            auxDf = df[['nodeClass', 'platform', m]].rename(columns={m: 'centrality'})
            auxDf['measure'] = m
            plotDf = auxDf if plotDf is None else pd.concat([plotDf, auxDf], ignore_index=False)

        g = sns.boxplot(data=plotDf, x='nodeClass', y='centrality', hue='measure', palette="Set2", showfliers=True, showmeans=True)
        g.set_yscale('log')
        plt.savefig(join(baseDir, f'centralityBoxPlot.png'))
        # plt.show()

        if saveCentralityToOS:
            logging.info('Saving dataframe to OS')
            df.to_pickle(join(baseDir, 'centralityDf.pickle'))

        if saveCentralityToDB:
            logging.info('Sending data to DB')
            client = MongoClient()
            db = client['digitalMe']
            collectionCont = db['content']
            for m in measurements:
                centralityPerNode = df[m].items()
                updateContentDocs(collectionCont, f'centrality-{m}', centralityPerNode)

    except Exception as ex:
        print(traceback.format_exc())
