import os
import sys
sys.path.append(os.path.join(os.getcwd(), '..'))  # Add src/ dir to import path
import traceback
import logging
from os.path import join
import subprocess

from libs.osLib import loadYaml

if __name__ == '__main__':
    # Set up logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Load config
    configDir = '../../configs/'
    config = loadYaml(join(configDir, 'main.yaml'))

    for script in config['pipeline']:
        try:
            logging.info(f'>>>>>>>>>>>>>>>>>> CURRENTLY RUNNING {script} <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
            subprocess.run(f'py {join(script)}')

        except Exception as ex:
            print(traceback.format_exc())
