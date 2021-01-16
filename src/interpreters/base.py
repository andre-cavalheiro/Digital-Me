import traceback
from pymongo import MongoClient
import logging

class baseInterpreter():

    def __init__(self, debug=False):
        self.originalData = None
        self.data = None
        self.debug = debug

    def load(self, path, name):
        pass

    def preProcess(self):
        # assert(self.originalData is not None)
        # self.data = self.data.fillna('')
        pass

    def transform(self, termsToIgnore):
        raise Exception('Not Implemented')

    def mergeTheSameContent(self, relevantKeysPerContentType):
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

                #if dataPoint['type'] == 'Video' and dataPoint['title'] == "Knowing bros [Ask Us Anything] Blackpink Ep. 87 English Sub":
                #    r=2

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

    def storeInDB(self, client):
        assert(self.data is not None)
        try:
            insertedDocs = client.insert_many(self.data)
            insertedIds = insertedDocs.inserted_ids
        except Exception as ex:
            print(traceback.format_exc())
        return insertedIds

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