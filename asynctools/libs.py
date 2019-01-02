import logging

LOG_LEVEL=logging.INFO

def loger(level=LOG_LEVEL):
    logging.basicConfig(level=level)
    return logging.getLogger(__file__)
