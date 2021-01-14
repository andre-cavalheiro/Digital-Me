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
    outputDir = join(baseDir, 'Centrality')

    calculateCentrality, loadFromOS = False, True
    saveCentralityToOS, saveCentralityToDB = False, False

    measurements = ['degree', 'betweennessParallel']    # 'closeness', 'katz'

    try:
        if calculateCentrality is True:
            # Load graph from OS
            G = nx.read_gpickle(join(baseDir, f'graph.gpickle'))

            # Power law plot
            degrees = [degree for id, degree in G.degree()]
            vz.degreeDistWithPowerLaw(degrees, join(outputDir, 'degreeDist'))

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
            df = na.calculateCentrality(G, df, measurements, saveAsWeGo=True, saveDir=outputDir)
        else:
            if loadFromOS is True:
                df = pd.read_csv(join(outputDir, f'centralityDf-DegBetw.csv'))
                nodeClasses = df.nodeClass.unique().tolist()

        # Save data if needed
        if saveCentralityToOS:
            logging.info('Saving dataframe to OS')
            df.to_csv(join(outputDir, 'centralityDf.csv'))

        if saveCentralityToDB:
            # TODO - this was not tested
            logging.info('Sending data to DB')
            client = MongoClient()
            db = client['digitalMe']
            collectionCont = db['content']
            for m in measurements:
                centralityPerNode = df[m].items()
                updateContentDocs(collectionCont, f'centrality-{m}', centralityPerNode)

        # Plot data

        # Centrality distributions per class
        # Inspired by: https://stackoverflow.com/questions/42004381/box-plot-of-a-many-pandas-dataframes
        plotDf = None
        for m in measurements:
            auxDf = df[['nodeClass', 'platform', m]].rename(columns={m: 'centrality'})
            auxDf['measure'] = m
            plotDf = auxDf if plotDf is None else pd.concat([plotDf, auxDf], ignore_index=False)

        # Change names for prettier plot
        measurementsMapping = {
            'degree': 'Degree Centrality',
            'betweennessParallel': 'Betweeness Centrality'
        }
        classMapping = {
            'time': 'Time',
            'content': 'Content',
            'tag': 'Tag',
            'location': 'Location'
        }
        plotDf['measure'] = plotDf['measure'].apply(lambda x: measurementsMapping[x])
        plotDf['nodeClass'] = plotDf['nodeClass'].apply(lambda x: classMapping[x])
        plotDf.rename({
            'measure': 'Centrality Measure',
            'centrality': 'Value',
            'nodeClass': 'Node Class',
        }, inplace=True, axis=1)

        # Make plot
        fig, ax = plt.subplots(figsize=(15, 15))
        g = sns.boxplot(ax=ax, data=plotDf, x='Node Class', y='Value', hue='Centrality Measure', palette="Set2",
                        showfliers=True, showmeans=True)
        g.set_yscale('log')

        # Save it
        plt.savefig(join(outputDir, f'centralityBoxPlot.png'), dpi=100)
        plt.close()

        # Another with matplotlib
        logAxis = True
        for m in measurements:
            data = [df[df.nodeClass == c][m].tolist() for c in ['tag']]
            vz.drawBoxPlots(data, nodeClasses, logAxis, join(outputDir, f'plt-{m}.png'), showfliers=True, meanline=True)

    except Exception as ex:
        print(traceback.format_exc())
