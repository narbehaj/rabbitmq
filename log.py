import logging

from config import *


class Logging:

    def __init__(self):
        self.logger = logging.getLogger('rabbit_client')
        self.logger.setLevel(logging.INFO)
        self.handler = logging.FileHandler(log_path)
        self.handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)

    def tolog(self, message, level='info'):
        if level == 'warn':
            self.logger.warn(message)
            return

        self.logger.info(message)


tolog = Logging().tolog
