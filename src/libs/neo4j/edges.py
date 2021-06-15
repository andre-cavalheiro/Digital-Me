import thinc
import traceback

thinc.registry.create("relationCreators")


@thinc.registry.relationCreators("simple.v1")
def simpleRelation():
    def callback(type, row, headAlias='head', tailAlias='tail'):
        statment = "MERGE ({})-[:{}]->({}) ".format(headAlias, type, tailAlias)
        return statment
    return callback

@thinc.registry.relationCreators("action.v1")
def actionRelation():
    def callback(type, row, headAlias='head', tailAlias='tail'):
        platform = row['service']

        # fixme
        if platform == 'Search':
            platform = 'Google Search'

        statment = "MERGE ({})-[:{} {{platform: \"{}\"}}]->({}) ".format(headAlias, type, platform, tailAlias)
        return statment
    return callback
