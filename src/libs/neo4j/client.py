import traceback
from libs.APIs.neo4jClient import *
from numpy import nan
from os.path import join

def init(address, user, password):
    session = connect(address, user, password)
    return session


def saveTripletToNeo4j(neo4jConfigFile):
    config = importAndMakeRegistry(neo4jConfigFile)

    def callback(df, ouputDir):
        c = config['connection']
        driver = GraphDatabase.driver(c['address'], auth=(c['user'], c['password']), encrypted=False) # fixme - encrypted
        session = driver.session()

        iterator = pl.loop(df)
        while True:
            try:
                index, row = next(iterator)
                headType, tailType, relationType = row['headType'], row['tailType'], row['relation']

                if headType not in config['nodeCreators'].keys():
                    raise Exception('Unknown node type {}'.format(headType))
                if tailType not in config['nodeCreators'].keys():
                    raise Exception('Unknown node type {}'.format(tailType))
                if relationType not in config['relationCreators'].keys():
                    raise Exception('Unknown relationship type {}'.format(relationType))

                headCreator = config['nodeCreators'][headType]
                tailCreator = config['nodeCreators'][tailType]
                relationCreator = config['relationCreators'][relationType]

                headStatment = headCreator('head', headType, row)
                tailStatment = tailCreator('tail', tailType, row)
                relStatment = relationCreator(relationType, row, headAlias='head', tailAlias='tail')

                session.write_transaction(executeStatment, headStatment + tailStatment + relStatment)
            except StopIteration:
                break
            except Exception as ex:
                print('> [Error] Unprocessed row {}'.format(index))
                print(traceback.format_exc())
                continue
        session.close()

        return {'outputDf': df}

    return callback

def convertToNetworkX(neo4jConfigFile):
    config = importAndMakeRegistry(neo4jConfigFile)

    def callback(*args):
        ouputDir = args[1]
        c = config['connection']
        driver = GraphDatabase.driver(c['address'], auth=(c['user'], c['password']), encrypted=False) # fixme - encrypted

        # statement = 'MATCH (n) RETURN n'
        statement = 'Match (n)-[r]->(m) Return n,r,m'
        result = singleStatment(driver, statement)
        G = graph_from_cypher(result.data())
        print(f'Created graph, num edges: {G.number_of_edges()}, num nodes: {G.number_of_nodes()}')
        nx.write_gpickle(G, join(ouputDir, 'graph.gpickle'))
        return
    return callback
