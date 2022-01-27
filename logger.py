import logging


def setup_logging(level=None):
    level = level or logging.INFO
    logger = logging.getLogger()
    logger.setLevel(level)
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s :: %(message)s')
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(level)
    logger.addHandler(stream_handler)
