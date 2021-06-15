import traceback
from pymongo import MongoClient
import logging

from libs.customExceptions import ContentWithMultipleLocations, ContentWithoutSource


class baseInterpreter():

    def __init__(self, debug=False):
        self.originalData = None
        self.data = None
        self.locationData = None
        self.debug = debug

    def load(self, path, name):
        pass

    def preProcess(self):
        # assert(self.originalData is not None)
        # self.data = self.data.fillna('')
        pass

    def transform(self, termsToIgnore):
        raise Exception('Not Implemented')

    def getContentData(self):
        return self.data

    def mergeSameContent(self, relevantKeysPerContentType):
        uniqueContent = {}
        try:

            for it, dataPoint in enumerate(self.data):

                # Do here to avoid another loop - If we're merging similiar content it might be visited at different times
                dataPoint['timestamp'] = [dataPoint['timestamp']]

                contentType = dataPoint['type']

                assert(contentType in relevantKeysPerContentType.keys())

                if relevantKeysPerContentType[contentType] is None:
                    # Unmergable type
                    continue

                contentKey = tuple([dataPoint[k] for k in relevantKeysPerContentType[contentType]])

                if contentKey in uniqueContent.keys():
                    uniqueContent[contentKey].append(it)
                else:
                    uniqueContent[contentKey] = [it]

            for contentKey, contantAppearendes in uniqueContent.items():

                if len(contantAppearendes) > 1:
                    # Nullify apearences to drop, while keeping their timestamp
                    newTimestamps = []
                    for i in contantAppearendes[1:]:
                        newTimestamps += self.data[i]['timestamp']
                        self.data[i] = None

                    # Add timestamps to the instance we're keeping
                    self.data[contantAppearendes[0]]['timestamp'] += newTimestamps

        except Exception as ex:
            print(traceback.format_exc())
            r = 1

        origLen = len(self.data)
        self.data = [d for d in self.data if d is not None]

        logging.info(f'Dropped a total of {origLen-len(self.data)} datapoints by merging')

    def getMinMaxTime(self):
        minDate, maxDate = None, None
        for d in self.data:
            if minDate is None and maxDate is None:
                minDate = d['timestamp']
                maxDate = d['timestamp']
            else:
                minDate = d['timestamp'] if minDate > d['timestamp'] else minDate
                maxDate = d['timestamp'] if maxDate < d['timestamp'] else maxDate
        return minDate, maxDate

    def addIds(self, documentIds):
        for datapoint, id in zip(self.data, documentIds):
            datapoint['_id'] = id

    def extractLocations(self, sourceKeysPerContentType, sourcesToIgnore, labelByLocationKey, sourceRelationship, platform=None):

        assert(platform is not None)

        output = {}
        err = []

        for content in self.data:
            try:

                sourceKeys = sourceKeysPerContentType[content['type']]

                if sourceKeys is None:
                    # This content type does not have a source
                    continue

                # For every possible source with reggard to this content type
                for sourceKey in sourceKeys:

                    if sourceKey not in content.keys():
                        raise ContentWithoutSource

                    # Get source
                    sources = content[sourceKey]

                    # Generalize to both lists of sources or single sources
                    sources = [sources] if not isinstance(sources, list) else sources

                    # Add it to sources list and associate it with the content
                    for src in sources:

                        if src in sourcesToIgnore:
                            continue

                        if src not in output.keys():
                            # New Source
                            output[src] = {
                                'type': labelByLocationKey[sourceKey],
                                'platform': platform,
                                'associatedContentTypes': [content['type']],
                                'associatedContent': [{
                                    'id': content['_id'],
                                    'relationshipType': sourceRelationship[sourceKey],

                                }],
                            }
                        else:
                            # Known Source
                            output[src]['associatedContent'].append({
                                    'id': content['_id'],
                                    'relationshipType': sourceRelationship[sourceKey]
                                })
                            if content['type'] not in output[src]['associatedContentTypes']:
                                output[src]['associatedContentTypes'].append(content['type'])
            except ContentWithoutSource:
                err.append(content['_id'])
            except Exception as ex:
                print(traceback.format_exc())
                continue

        if len(err) > 0:
            logging.warning(f'Found {len(err)} documents without source')

        self.locationData = [dict(dt, label=k) for k, dt in output.items()]
        return self.locationData

