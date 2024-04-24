import os
import logging

LOG_MODE = os.getenv("LOG_MODE", "FILE")

_logger = logging.Logger("redash2dqsql")
_logger.setLevel(logging.INFO)
_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

if LOG_MODE == "FILE":
    _handler = logging.FileHandler("redash2dqsql.log")
    _handler.setLevel(logging.INFO)
    _logger.addHandler(_handler)

for h in _logger.handlers:
    h.setFormatter(_formatter)

LOGGER = _logger
