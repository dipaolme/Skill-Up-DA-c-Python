import os
# from decouple import config
from decouple import config
from datetime import timedelta
import logging
import decouple

import logging.config
from sqlCommandB import createPath,identExt


def configDag():


    default_args = {
        'owner': 'Breyner',
        'retries': 5,
        'retry_delay': timedelta(minutes=10),
    }

    POSTGRES_CONN_ID = decouple.config("POSTGRES_CONN_ID")
    print(".ENv connect->", POSTGRES_CONN_ID)
    # logger.info(".ENv connect-> %s", POSTGRES_CONN_ID)
    # logger.info("data: %s", name)
    return default_args, POSTGRES_CONN_ID


# def configLog(name):
def configLog(name):
    # print("Configlog")
 
    
    pathconf = createPath("assets")
    pathRoot = identExt(pathconf, ".cfg")
    # print(pathconf)
    # print(pathRoot)
    n = pathRoot.index(name+".cfg")
    pathRoot = pathRoot[n]
    path = pathconf+"/"+pathRoot
    # print(path)
    
    logging.config.fileConfig(path,disable_existing_loggers=False)

    logger = logging.getLogger(name)


    # print(logger)

    # # log something
    # logging.info("congfilog prueba")
    # logger.debug('debug message')
    # logger.info('info message')
    # logger.warn('warn message')
    # logger.error('error message')
    # logger.critical('critical message')


    

    

    return logger


# print(POSTGRES_CONN_ID)
if __name__ == "__main__":
    configDag()
    configLog("GBUNComahue_dag_elt")
    # print(d,s)
