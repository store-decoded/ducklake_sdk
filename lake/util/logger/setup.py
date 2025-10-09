import logging.config as logconf
import os
from logging import Logger, getLogger

import yaml


def setup_logging() -> Logger:
	"""
	Set up logging config.
	Returns logger object.
	"""
	config_path = os.getenv(key="LOG_CONFIG_FILE", default="resources/log_config.yml")
	logger_name = os.getenv(key="LOGGER_NAME", default="development")
	with open(file=config_path) as f:
		config = yaml.safe_load(f)
	logconf.dictConfig(config)
	return getLogger(logger_name)


logger = setup_logging()
