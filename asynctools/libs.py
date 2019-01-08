import logging
# from termcolor import colored
LOG_LEVEL=logging.INFO

def loger(level=LOG_LEVEL):
    logging.basicConfig(level=level, format='%(levelname)s[%(asctime)s]| %(filename)s: %(lineno)d :  %(message)s')
    return logging.getLogger(__file__)
