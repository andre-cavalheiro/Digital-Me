from os import getcwd
import sys
sys.path.append(getcwd() + '/..')   # Add src/ dir to import path
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
import libs.visualization as vz
from libs.osLib import loadYaml
from libs.mongoLib import updateContentDocs, getFromId, getContentDocsPerPlatform

if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    
    # Load config
    configDir = '../../configs/'
    config = loadYaml(join(configDir, 'main.yaml'))

    calculateCentrality, loadFromOS = False, True
    saveCentralityToOS, saveCentralityToDB = False, False

    PERIOD = 'week'
    measurements = ['degree', 'betweennessParallel']

    try:
        if calculateCentrality is True:
            # Load graph from OS
            fileName = f'graph.gpickle' if PERIOD == 'day' else f'graph-{PERIOD}.gpickle'
            G = nx.read_gpickle(join(config['dataDir'], ))

            # Power law plot
            degrees = [degree for id, degree in G.degree()]
            vz.degreeDistWithPowerLaw(degrees, join(config['centralityDir'], f'degreeDist-{PERIOD}'))

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
            df = na.calculateCentrality(G, df, measurements, saveAsWeGo=True, saveDir=config['centralityDir'])
        else:
            if loadFromOS is True:
                fileName = f'centralityDf.csv' if PERIOD == 'day' else f'centralityDf-{PERIOD}.csv'
                df = pd.read_csv(join(config['centralityDir'], fileName))
                nodeClasses = df.nodeClass.unique().tolist()
            else:
                raise Exception('Initial conditions conflicting - where should i get the graph from?')

        # Save data if needed
        if saveCentralityToOS:
            logging.info('Saving dataframe to OS')
            fileName = f'centralityDf.csv' if PERIOD == 'day' else f'centralityDf-{PERIOD}.csv'
            df.to_csv(join(config['centralityDir'], fileName))

        if saveCentralityToDB:
            # TODO - this was not tested
            from libs.mongoLib import updateContentDocs
            logging.info('Sending data to DB')
            client = MongoClient()
            db = client['digitalMe']
            collectionCont = db['content']
            for m in measurements:
                centralityPerNode = df[m].items()
                updateContentDocs(collectionCont, f'centrality-{m}', centralityPerNode)

        # Set up DB
        client = MongoClient()
        db = client['digitalMe']
        collectionCont = db['content']
        collectionEnt = db['entities']
        collectionSource = db['locations']

        #info, matchingColl = getFromId(id, collectionEnt, collectionSource)
        #associatedContent = info['associatedContent']
        #entityLabel = info['label'] if matchingColl is collectionSource else info['mentionForms'][0]


        # Plot data

        # Centrality distributions per class
        # Inspired by: https://stackoverflow.com/questions/42004381/box-plot-of-a-many-pandas-dataframes
        plotDf = None
        df = df.rename(columns={'Unnamed: 0': 'id'})
        df = df.set_index('id')

        for m in measurements:
            auxDf = df[['nodeClass', 'platform', m]].rename(columns={m: 'centrality'})
            auxDf = auxDf[auxDf['nodeClass'] == 'tag']

            dbpediaTypes = []
            for index, row in auxDf.iterrows():
                info, matchingColl = getFromId(index, collectionEnt, collectionSource)
                dbpediaTypes.append(info['id'])

            dbpediaTypes = ['With\n DBpedia Types' if t.startswith('Q') else
                            'Without\n DBpedia Types' for t in dbpediaTypes]
            auxDf['dbpediaType'] = dbpediaTypes
            auxDf['measure'] = m

            plotDf = auxDf if plotDf is None else pd.concat([plotDf, auxDf], ignore_index=False)

        # Change names for prettier plot
        measurementsMapping = {
            'degree': 'Degree Centrality',
            'betweennessParallel': 'Betweeness Centrality'
        }
        '''
        classMapping = {
            'time': 'Time',
            'content': 'Content',
            'tag': 'Tag',
            'location': 'Location',
            'spatial': 'Location'
        }
        '''
        plotDf['measure'] = plotDf['measure'].apply(lambda x: measurementsMapping[x])
        # plotDf['nodeClass'] = plotDf['nodeClass'].apply(lambda x: classMapping[x])

        plotDf.rename({
            'measure': 'Centrality Measure',
            'centrality': 'Value',
            'dbpediaType': 'Linkage',
        }, inplace=True, axis=1)

        # Make plot
        sns.set(font_scale=2)
        fig, ax = plt.subplots(figsize=(12, 12))
        g = sns.boxplot(ax=ax, data=plotDf, x='Linkage', y='Value', hue='Centrality Measure', palette="Set2",
                        showfliers=True, showmeans=True)
        g.set_yscale('log')

        # Save it
        fileName = f'centralityBoxPlotTags.png' if PERIOD == 'day' else f'centralityBoxPlotTags-{PERIOD}.png'
        plt.savefig(join(config['centralityDir'], fileName), dpi=100)
        plt.close()

    except Exception as ex:
        print(traceback.format_exc())
