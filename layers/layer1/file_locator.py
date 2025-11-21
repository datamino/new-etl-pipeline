# layers/layer1/file_locator.py
# ---------------------------------------------------------
# Layer 1 – Step 1: Locate the raw input CSV.GZ file.
# Loads configuration from config.yaml.
# Uses centralized logging via util.logger.get_logger().
# ---------------------------------------------------------

from pathlib import Path
from util.config_loader import load_config
from util.logger import get_logger

# Full module name for logging (per your choice Option A)
logger = get_logger("layer1.file_locator")


def build_filename_for_date(processing_date: str) -> str:
    """
    Build filename using filename_pattern in config.yaml.

    Example:
        processing_date = "2025-11-21"
        pattern = "mc_us_new_combined_{date}.csv.gz"
        returns: "mc_us_new_combined_20251121.csv.gz"
    """
    cfg = load_config()
    pattern = cfg["layer1"].get("filename_pattern", "mc_us_new_combined_{date}.csv.gz")
    token = processing_date.replace("-", "")
    filename = pattern.format(date=token)

    logger.debug(f"build_filename_for_date: pattern={pattern}, token={token}, filename={filename}")
    return filename


def locate_raw_file(processing_date: str) -> Path:
    """
    Locate the raw gzip CSV file in input_dir.
    Raises FileNotFoundError if not found.

    Returns:
        Path to CSV.GZ file
    """
    cfg = load_config()
    input_dir = Path(cfg["layer1"]["input_dir"])

    filename = build_filename_for_date(processing_date)
    full_path = input_dir / filename

    logger.info(f"Locating raw file for date {processing_date}: {full_path}")

    if not full_path.exists():
        logger.error(f"Raw input file NOT FOUND: {full_path.resolve()}")
        raise FileNotFoundError(f"Layer1: raw input file not found: {full_path.resolve()}")

    logger.info(f"Found raw file: {full_path.resolve()}")
    return full_path
