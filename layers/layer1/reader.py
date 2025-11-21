# layers/layer1/reader.py
# ---------------------------------------------------------
# Layer 1 – Step 2: Read the raw CSV.GZ file using Polars.
# Uses lazy scan + streaming collect for memory-safe performance.
# Falls back to read_csv() if scan_csv fails.
#
# Logging: Centralized (Option C), module name: layer1.reader
# ---------------------------------------------------------


from pathlib import Path
import polars as pl

from util.logger import get_logger

logger = get_logger("layer1.reader")


def read_raw_with_polars(raw_path: Path, infer_rows: int = 50_000) -> pl.DataFrame:
    """
    Read compressed MarketCheck CSV.GZ using Polars.

    - TRY 1: scan_csv (FASTEST)
    - TRY 2: read_csv (SAFE FALLBACK)
    """

    raw_path = Path(raw_path)
    logger.info(f"Reading raw CSV.GZ with Polars: {raw_path}")

    # ---------------------------------------------------------
    # TRY 1 — scan_csv (no comment_char, fully compatible)
    # ---------------------------------------------------------
    try:
        lazy_df = pl.scan_csv(
            str(raw_path),
            has_header=True,
            infer_schema_length=infer_rows,
            ignore_errors=True,
        )

        df = lazy_df.collect(streaming=True)

        logger.info(
            f"scan_csv SUCCESS → {df.height}: rows={df.height}, cols={len(df.columns)}"
        )

        return df

    except Exception as e:
        logger.warning(f"scan_csv failed → {e}. Trying fallback read_csv...")

    # ---------------------------------------------------------
    # TRY 2 — read_csv fallback
    # ---------------------------------------------------------
    try:
        df_fallback = pl.read_csv(
            str(raw_path),
            ignore_errors=True,
        )

        # Validate type — MUST be a DataFrame
        if not isinstance(df_fallback, pl.DataFrame):
            raise TypeError(
                f"read_csv returned non-DataFrame: {type(df_fallback)}"
            )

        logger.info(
            f"Fallback read_csv SUCCESS → rows={df_fallback.height}, cols={len(df_fallback.columns)}"
        )

        return df_fallback

    except Exception as fatal:
        logger.error(f"Fallback read_csv FAILED → {fatal}")
        raise RuntimeError(f"❌ Unable to read CSV.GZ file: {raw_path}") from fatal
