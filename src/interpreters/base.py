import traceback
from pymongo import MongoClient

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

    def storeInDB(self, client):
        assert(self.data is not None)
        try:
            client.insert_many(self.data)       # todo - this should be changed to update to avoid duplicates.
        except Exception as ex:
            print(traceback.format_exc())

