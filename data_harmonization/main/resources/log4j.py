import logging
import os
from typing import Union

from data_harmonization.main.resources import config as config_


class Logger(object):
    LOGGING_DIR = config_.logging_dir

    def __init__(self, name):
        name = name.replace(".log", "")
        logger = logging.getLogger(
            "log_namespace.%s" % name
        )  # log_namespace can be replaced with your namespace
        logger.setLevel(logging.DEBUG)
        self.loglevel = dict(NOTSET=0, DEBUG=10, INFO=20, WARN=30, ERROR=40, CRITICAL=50)
        if not logger.handlers:
            file_name = os.path.join(
                self.LOGGING_DIR, "%s.log" % name
            )  # usually I keep the LOGGING_DIR defined in some global settings file
            handler = logging.FileHandler(file_name)
            formatter = logging.Formatter("%(asctime)s %(levelname)s:%(name)s %(message)s")
            handler.setFormatter(formatter)
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)
        self._logger = logger

    def get(self) -> "Logger":
        return self

    def set_loglevel(self, level: Union[int, str]) -> None:
        """NOTSET=0, DEBUG=10, INFO=20, WARN=30, ERROR=40, and CRITICAL=50"""
        self._logger.setLevel(level)

    def log(self, level: Union[int, str], msg: str, **kwargs) -> None:
        """log the message

        Parameters
        ----------
        level: int, str
            must be of NOTSET=0, DEBUG=10, INFO=20, WARN=30, ERROR=40, and CRITICAL=50
        msg: str
            message to be logged
        """
        if not isinstance(level, int):
            level = self.loglevel.get(level)
        if kwargs:
            self._logger.log(level, msg, kwargs)
            return
        self._logger.log(level, msg)


if __name__ == "__main__":
    logger = Logger("test")
    logger.log("INFO", "testing the logger object")
