from os import getcwd
import sys
sys.path.append(getcwd() + '/..')   # Add src/ dir to import path
import traceback
import logging
from os.path import join
from datetime import date, timedelta, datetime
from collections import Counter
from datetime import datetime

import networkx as nx
import pandas as pd
import numpy as np
import seaborn as sns
import calmap
import matplotlib.pyplot as plt
from pymongo import MongoClient

from libs.mongoLib import updateContentDocs, getFromId, getContentDocsPerPlatform
import libs.visualization as vz
import libs.osLib as ol

if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Load config
    baseDir, outputDir = '../../data', '../../data/Plots'
    configDir = '../../configs/'

    config = ol.loadYaml(join(configDir, 'main.yaml'))

    IDs = ['60199a05768eb6dcf79c67b3']    # Only valid: Tags and Sources

    # Set up DB
    client = MongoClient()
    db = client['digitalMe']
    collectionCont = db['content']
    collectionEnt = db['entities']
    collectionSource = db['locations']

    contentList = list(getContentDocsPerPlatform(collectionCont, list(config['platforms'].keys())))
    contentDict = {x['_id']: x for x in contentList}

    try:
        for id in IDs:

            info, matchingColl = getFromId(id, collectionEnt, collectionSource)
            associatedContent = info['associatedContent']
            entityLabel = info['label'] if matchingColl is collectionSource else info['mentionForms'][0]

            daysOfAccess = [contentDict[c['id']]['timestamp'] for c in associatedContent]
            daysOfAccess = [item for sublist in daysOfAccess for item in sublist]   # Flatten list of lists

            daysOfAccess = pd.Series(daysOfAccess)
            # daysOfAccessDT = pd.to_datetime(daysOfAccess).dt
            # daysOfAccessDT = pd.Series(daysOfAccess.values, index=pd.to_datetime(daysOfAccess, unit='s'))
            dayFrequencies = daysOfAccess.value_counts()

            calmap.calendarplot(dayFrequencies, monthticks=3, daylabels='MTWTFSS', dayticks=True, fillcolor='grey', linewidth=0, fig_kws=dict(figsize=(16, 8)))
            plt.title(entityLabel)
            plt.savefig(join(outputDir, f'calendarHeatMap-{entityLabel}.png'))
    except Exception as ex:
        print(traceback.format_exc())
