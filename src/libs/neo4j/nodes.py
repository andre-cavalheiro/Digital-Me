import thinc
import traceback
from numpy import nan
import pandas as pd
from ast import literal_eval
from libs.utils import standardizeLabel


thinc.registry.create("nodeCreators")


@thinc.registry.nodeCreators("TimeNodes.v1")
def createTimeNode():
    def callback(role, type, row):
        if role != 'head' and role != 'tail':
            raise Exception('Unknown node type: \"{}\"'.format(role))
        alias = role

        labelKey = 'tailLabel' if role == 'tail' else 'headLabel'
        date = pd.to_datetime(row[labelKey])
        date = date.strftime('%Y-%m-%d')

        statement = "MERGE ({}:{} {{" \
                    "date: \"{}\"" \
                    "}}) ".format(alias, type, date)
        return statement
    return callback


# fixme - should add list with used mentions
@thinc.registry.nodeCreators("EntityNodes.v1")
def createEntityNode(labelName):
    def callback(role, type, row):
        if role != 'head' and role != 'tail':
            raise Exception('Unknown node type: \"{}\"'.format(role))
        alias = role

        labelKey = 'tailLabel' if role == 'tail' else 'headLabel'
        label = standardizeLabel(row[labelKey])
        wikiID = row['entityID'] if row['entityID'] != nan else 'UNK'
        entityType = row['entityType']
        dbpediaTypes = literal_eval(row['dbpediaTypes'])

        statement = "MERGE ({}:{} {{" \
                    "{}: \"{}\"," \
                    "wikiID: \"{}\"," \
                    "rosetteTypes: \"{}\", " \
                    "dbpediaTypes: {} " \
                    "}}) "\
            .format(alias, type, labelName, label, wikiID, entityType, dbpediaTypes)
        return statement
    return callback

@thinc.registry.nodeCreators("TweetNodes.v1")
def createTweetNode(labelName):
    def callback(role, type, row):
        if role != 'head' and role != 'tail':
            raise Exception('Unknown node type: \"{}\"'.format(role))
        alias = role

        labelKey = 'tailLabel' if role == 'tail' else 'headLabel'
        label = standardizeLabel(row[labelKey])
        likes = row['likeCount']
        retweets = row['retweetCount']

        statement = "MERGE ({}:{} {{" \
                    "{}: \"{}\"," \
                    "likes: {}," \
                    "retweets: {} " \
                    "}}) "\
            .format(alias, type, labelName, label, likes, retweets)
        return statement
    return callback


@thinc.registry.nodeCreators("SingleLabelNode.v1")
def createSingleLabelNode(labelName):
    def callback(role, type, row):
        if role != 'head' and role != 'tail':
            raise Exception('Unknown node type: \"{}\"'.format(role))

        alias = role

        labelKey = 'tailLabel' if role == 'tail' else 'headLabel'
        label = standardizeLabel(row[labelKey])

        statement = "MERGE ({}:{} {{" \
                    "{}: \"{}\" " \
                    "}}) ".format(alias, type, labelName, label)
        return statement
    return callback


@thinc.registry.nodeCreators("QueryNode.v1")
def createQueryNode(labelName):
    def callback(role, type, row):
        if role != 'head' and role != 'tail':
            raise Exception('Unknown node type: \"{}\"'.format(role))

        alias = role
        labelKey = 'tailLabel' if role == 'tail' else 'headLabel'
        label = standardizeLabel(row[labelKey])
        service = row['service']

        statement = "MERGE ({}:{} {{" \
                    "{}: \"{}\", " \
                    "{}: \"{}\" " \
                    "}}) ".format(alias, type, labelName, label, "service", service)
        return statement
    return callback

@thinc.registry.nodeCreators("WebsiteNode.v1")
def createWebsiteNode(labelName):
    def callback(role, type, row):
        if role != 'head' and role != 'tail':
            raise Exception('Unknown node type: \"{}\"'.format(role))

        alias = role

        labelKey = 'tailLabel' if role == 'tail' else 'headLabel'
        label = row[labelKey]   # Don't clean website strings (you want the punctuation)
        statement = "MERGE ({}:{} {{" \
                    "{}: \"{}\" " \
                    "}}) ".format(alias, type, labelName, label)
        return statement

    return callback
