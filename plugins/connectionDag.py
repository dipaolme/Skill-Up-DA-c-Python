from decouple import config
from datetime import timedelta
import logging
def configDag():
    logging.basicConfig(filename='myapp.log',
                        format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)
    default_args = {
        'owner': 'brey',
        'retries': 5,
        'retry_delay': timedelta(minutes=10),
    }

    POSTGRES_CONN_ID = config("POSTGRES_CONN_ID")
    print(".ENv connect->",POSTGRES_CONN_ID)
    logging.info(".ENv connect-> %s", POSTGRES_CONN_ID)
    # logging.info("data: %s", name)
    return default_args,POSTGRES_CONN_ID


# print(POSTGRES_CONN_ID)
