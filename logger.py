import logging


def get_logger(output_dir):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers = []  # clear previous handlers

    # create console handler and set level
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
