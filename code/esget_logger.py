""" initializes the global logging environment:
  root logger, level, formatting, filehandler"""
import logging


class Logging(object):
    def __init__(self, config):
        self.logfile = config.get('Paths', 'logfile')
        self.loglevel = getattr(logging, config.get('Tuning', 'loglevel'))
        self.logger = logging.getLogger()
        self.logger.setLevel(self.loglevel)
        fh = logging.FileHandler(self.logfile, mode='a')
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
