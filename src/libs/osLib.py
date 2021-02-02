import pickle
import scipy.sparse
import numpy as np
import yaml


def loadYaml(path):
    with open(path) as f:
        # use safe_load instead load
        configData = yaml.safe_load(f)
    return configData

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


def loadNumpy(name):
    M = np.load(name)
    return M


def saveNumpy(dt, name):
    np.save(name, dt)