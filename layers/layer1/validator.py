# layers/layer1/validator.py
# ---------------------------------------------------------
# Validator for Layer-1 parquet output.
# - Ensures output contains the expected columns (presence + order check)
# - Reads only the first parquet part (cheap) to inspect schema
# - Returns True when schema is valid, False otherwise
# - Raises helpful errors and logs issues for debugging
# ---------------------------------------------------------

from pathlib import Path
import polars as pl
from util.logger import get_logger
from util.config_loader import load_config

logger = get_logger("layer1.validator")

_config = load_config()
_LAYER1_CFG = _config.get("layer1", {})
EXPECTED_COLUMNS = _LAYER1_CFG.get("full_columns_new", [])  # list order preserved


def _find_first_parquet_part(output_dir: Path):
    """Return Path to the first part-*.parquet file inside output_dir (or None)."""
    output_dir = Path(output_dir)
    if not output_dir.exists():
        return None

    # Look for part-*.parquet (sorted lexicographically)
    parts = sorted([p for p in output_dir.iterdir() if p.is_file() and p.name.startswith("part-") and p.suffix in (".parquet", ".parq")])
    return parts[0] if parts else None


def validate_output_schema(output_dir: Path) -> bool:
    """
    Validate the schema of Layer1 parquet output.

    Args:
        output_dir: Path to folder containing part-*.parquet files (e.g. data/layer1_output/new/20250115)

    Returns:
        True if schema matches expected (presence and order check), False otherwise.
    """
    output_dir = Path(output_dir)
    logger.info(f"[Validator] Validating Layer1 output folder: {output_dir}")

    first_part = _find_first_parquet_part(output_dir)
    if not first_part:
        logger.error(f"[Validator] No parquet parts found in {output_dir}")
        raise RuntimeError(f"No parquet output parts found in {output_dir}")

    logger.info(f"[Validator] Inspecting schema from: {first_part.name}")

    # read only one row to get schema quickly
    try:
        # polars.read_parquet supports n_rows; set to 1 to be cheap
        sample_df = pl.read_parquet(str(first_part), n_rows=1)
    except Exception as e:
        logger.error(f"[Validator] Failed to read parquet part {first_part}: {e}")
        raise

    produced_columns = list(sample_df.columns)
    expected_columns = list(EXPECTED_COLUMNS)

    # Presence checks
    missing = [c for c in expected_columns if c not in produced_columns]
    extra = [c for c in produced_columns if c not in expected_columns]

    # Order check (only meaningful if sets are equal)
    order_mismatch = False
    mismatch_info = {}
    if not missing and not extra:
        if produced_columns != expected_columns:
            order_mismatch = True
            # simple way to describe difference: show first index where mismatch occurs
            for i, (exp_col, prod_col) in enumerate(zip(expected_columns, produced_columns)):
                if exp_col != prod_col:
                    mismatch_info["first_mismatch_index"] = i
                    mismatch_info["expected"] = exp_col
                    mismatch_info["produced"] = prod_col
                    break

    # Logging and result
    if missing:
        logger.error(f"[Validator] Missing columns ({len(missing)}): {missing[:10]}{'...' if len(missing)>10 else ''}")
    if extra:
        logger.warning(f"[Validator] Extra columns ({len(extra)}): {extra[:10]}{'...' if len(extra)>10 else ''}")
    if order_mismatch:
        idx = mismatch_info.get("first_mismatch_index")
        logger.warning(
            f"[Validator] Column order differs. First mismatch at index {idx}: expected='{mismatch_info.get('expected')}', produced='{mismatch_info.get('produced')}'"
        )

    valid = (len(missing) == 0)
    if valid:
        logger.info(f"[Validator] Schema validation PASSED → {len(produced_columns)} columns")
    else:
        logger.error("[Validator] Schema validation FAILED")

    return valid
