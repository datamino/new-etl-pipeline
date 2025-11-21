# util/logger.py
# ---------------------------------------------------------
# Central logging configuration for the ETL pipeline
# Provides get_logger(name) for all modules.
# Logs to console + rotating file logs/layer1.log
# ---------------------------------------------------------

import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

# Ensure logs/ directory exists
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "layer1.log"

# Common log format
FORMATTER = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"


def _configure_root_logger():
    """
    Configure the root logger exactly once.
    All layer modules inherit this configuration.
    """
    root = logging.getLogger()
    if root.handlers:
        return  # already configured

    root.setLevel(logging.INFO)

    # ---- Console Handler ----
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(FORMATTER, datefmt=DATEFMT))
    root.addHandler(console_handler)

    # ---- File Handler (Rotating) ----
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=50 * 1024 * 1024, backupCount=10, encoding="utf-8"
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(FORMATTER, datefmt=DATEFMT))
    root.addHandler(file_handler)


# Configure once at import
_configure_root_logger()


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger instance for a module.
    Example:
        logger = get_logger("layer1.reader")
    """
    return logging.getLogger(name)
