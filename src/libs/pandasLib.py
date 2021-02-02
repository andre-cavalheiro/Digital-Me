import pandas as pd


def unrollListAttr(df, targetColumn, otherColumns=[], newAttrName='value', indexName='index'):
    '''
    Transform column containing list values
    :param df:
    :param targetColumn:
    :param otherColumns:
    :return:
    '''

    # One row per entity extracted, keeping the document id in each
    auxdf = df[targetColumn].apply(pd.Series, dtype='object').reset_index()
    auxdf = auxdf.melt(id_vars=indexName)
    auxdf = auxdf.dropna()[[indexName, 'value']]
    auxdf = auxdf.set_index(indexName)

    if len(otherColumns) > 0:
        df = pd.merge(auxdf, df[otherColumns], left_index=True, right_index=True)
        df.reset_index(inplace=True, drop=True)
    else:
        df = auxdf
    df.rename({'value': newAttrName}, axis=1, inplace=True)

    return df


def unrollListOfDictsAttr(df, targetColumn, otherColumns=[], newAttrName='value', indexName='index'):
    df = unrollListAttr(df, targetColumn, otherColumns=otherColumns, newAttrName=newAttrName, indexName=indexName)

    # Unpack dictionary
    k = pd.json_normalize(df[newAttrName].tolist())
    df = df.join(k)
    df.drop(newAttrName, axis=1, inplace=True)
    return df