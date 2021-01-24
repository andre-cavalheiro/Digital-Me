import pickle
import scipy.sparse

def loadPickle(name):
    with open(name, 'rb') as f:
        data = pickle.load(f)
    return data


def savePickle(dt, name):
    with open(name, 'wb') as f:
        pickle.dump(dt, f)


def loadSparce(name):
    M = scipy.sparse.load_npz(name)
    return M


def saveSparce(dt, name):
    scipy.sparse.save_npz(name, dt)


