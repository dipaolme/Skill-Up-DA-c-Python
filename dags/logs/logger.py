import os 
import logging

WORKING_DIR = os.getcwd()
LOGS_DIR = os.path.join(WORKING_DIR, 'tests')  
logger = logging.getLogger('GIJujuy')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')

handler = logging.FileHandler(os.path.join(LOGS_DIR, 'GAUFlores.log'))
handler.setFormatter(formatter)

logger.addHandler(handler)