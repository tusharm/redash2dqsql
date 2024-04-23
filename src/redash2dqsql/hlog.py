import os
import logging

LOG_MODE = os.getenv("LOG_MODE", "FILE")

if LOG_MODE == "FILE":
    _logger = logging.Logger("redash2dqsql")
    _logger.setLevel(logging.INFO)
    _handler = logging.FileHandler("redash2dqsql.log")
    _handler.setLevel(logging.INFO)
    _logger.addHandler(_handler)
else:
    _logger = logging.getLogger("redash2dqsql")
    _logger.setLevel(logging.INFO)


LOGGER = _logger
