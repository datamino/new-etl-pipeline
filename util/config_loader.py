# util/config_loader.py
# ---------------------------------------------------------
# Loads YAML configuration for the new ETL pipeline.
# All pipeline components (layers, flows, tasks) import from here.
#
# Uses Pathlib for clean path handling.
# ---------------------------------------------------------

from pathlib import Path
import yaml


CONFIG_PATH = Path("config.yaml")


def load_config() -> dict:
    """
    Load config.yaml and return its content as a dict.

    Returns:
        dict: Nested config structure (layer1, layer2, global, etc.)
    """
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found at: {CONFIG_PATH.resolve()}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config
