import yaml


def loadYaml(path):
    with open(path) as f:
        # use safe_load instead load
        configData = yaml.safe_load(f)
    return configData