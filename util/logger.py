# util/logger.py
"""
Centralized logging utilities for the ETL pipeline.

Usage:
    from util.logger import get_logger
    logger = get_logger("module.name")

Features:
  - Rotating file handler (keeps last N MBs)
  - Console handler (stream)
  - Read LOG_LEVEL and LOG_DIR from environment variables
  - UTF-8 safe, path handling with pathlib
  - Prevents double-configuration if module imported multiple times
"""

from pathlib import Path
import logging
import logging.handlers
import os
from typing import Optional

# Configuration via environment variables (safe defaults)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_FILENAME = os.getenv("LOG_FILENAME", "pipeline.log")
MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MB per file
BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))  # keep 5 rotated logs
DATEFMT = "%Y-%m-%d %H:%M:%S"
FORMATTER = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"

# Ensure log directory exists
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / LOG_FILENAME

# Root logger initialization guard
_configured = False


def _configure_root_logger():
    global _configured
    if _configured:
        return

    # Root logger
    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)

    # Remove default handlers to avoid duplicates (safe-guard)
    for h in list(root.handlers):
        root.removeHandler(h)

    formatter = logging.Formatter(FORMATTER, datefmt=DATEFMT)

    # Rotating file handler (UTF-8)
    file_handler = logging.handlers.RotatingFileHandler(
        filename=str(LOG_PATH),
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(LOG_LEVEL)

    # Console handler (stream)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(LOG_LEVEL)

    root.addHandler(file_handler)
    root.addHandler(console_handler)

    # Avoid noisy third-party logs (adjust as needed)
    logging.getLogger("httpx").setLevel("WARNING")
    logging.getLogger("urllib3").setLevel("WARNING")
    logging.getLogger("botocore").setLevel("WARNING")

    _configured = True


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Returns a configured logger with the given module name.
    Example: logger = get_logger("layers.layer1.reader")
    """
    _configure_root_logger()
    return logging.getLogger(name if name else __name__)
