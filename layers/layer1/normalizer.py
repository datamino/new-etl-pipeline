# layers/layer1/normalizer.py
# ---------------------------------------------------------
# Layer 1 – Step 3: Normalize the DataFrame to FULL_COLUMNS_NEW.
#
# Responsibilities (match old pipeline exactly):
#   - Add missing columns (filled with "")
#   - Drop extra columns
#   - Reorder columns to match schema
#   - Cast all columns to Utf8 (string) to match old pandas dtype=str behavior
#
# Logging is handled centrally via util/logger.py.
# Module name: layer1.normalizer
# ---------------------------------------------------------

from typing import List
import polars as pl

from util.config_loader import load_config
from util.logger import get_logger

# Full module logger name
logger = get_logger("layer1.normalizer")


def load_schema_from_config() -> List[str]:
    """
    Load FULL_COLUMNS_NEW from config.yaml.
    This is the strict schema we must match.
    """
    cfg = load_config()
    schema = cfg["layer1"]["full_columns_new"]
    logger.debug(f"Loaded schema with {len(schema)} columns from config.yaml")
    return schema


def normalize_dataframe_to_schema(df: pl.DataFrame, schema_cols: List[str]) -> pl.DataFrame:
    """
    Normalize df to EXACTLY the columns defined in schema_cols.

    Steps:
      1. Cast all existing columns to Utf8
      2. Add missing columns with empty string
      3. Drop extra columns
      4. Reorder to match schema_cols

    Returns:
        Polars DataFrame fully normalized
    """
    total_rows = df.height
    original_cols = df.columns

    logger.info(
        f"Normalizing DataFrame (rows={total_rows:,}, cols={len(original_cols)}) "
        f"to schema ({len(schema_cols)} columns)"
    )

    # ---------------------------------------------------------
    # Step 1 — Cast all columns to Utf8 (string)
    # ---------------------------------------------------------
    for col in original_cols:
        try:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))
        except Exception:
            df = df.with_columns(pl.col(col).cast(pl.Utf8, strict=False))
    logger.debug("All existing columns cast to Utf8 (string)")

    # ---------------------------------------------------------
    # Step 2 — Add missing columns
    # ---------------------------------------------------------
    existing = set(df.columns)
    missing = [c for c in schema_cols if c not in existing]

    if missing:
        logger.info(f"Adding {len(missing)} missing columns: {missing}")
        # Create missing columns filled with empty strings
        missing_map = {col: [""] * total_rows for col in missing}
        missing_df = pl.DataFrame(missing_map).with_columns(
            [pl.col(col).cast(pl.Utf8) for col in missing]
        )
        df = pl.concat([df, missing_df], how="horizontal")

    # ---------------------------------------------------------
    # Step 3 — Drop extra columns
    # ---------------------------------------------------------
    new_existing = set(df.columns)
    extra = [c for c in new_existing if c not in schema_cols]

    if extra:
        logger.info(f"Dropping {len(extra)} extra columns: {extra}")
        df = df.select([col for col in df.columns if col in schema_cols])

    # ---------------------------------------------------------
    # Step 4 — Reorder to match schema exactly
    # ---------------------------------------------------------
    df = df.select(schema_cols)
    logger.debug("Column ordering normalized to schema")

    logger.info(
        f"Normalization complete. Final shape: {df.height:,} rows × {len(df.columns)} columns"
    )

    return df
