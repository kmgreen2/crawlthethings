import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


class Logger:
    def __init__(self):
        pass

    def info(self, msg):
        logging.info(msg)

    def debug(self, msg):
        logging.debug(msg)

    def error(self, msg):
        logging.error(msg)

    def warning(self, msg):
        logging.warning(msg)
