# layers/layer1/reader.py
# ---------------------------------------------------------
# Layer 1 – Step 2: Read raw CSV.GZ using Polars, safely.
# Windows-friendly, memory-safe, compatible with all Polars
# versions (0.17 → 1.0+).
#
# Strategy:
#   1) Decompress to a temp CSV file (1MB chunks)
#   2) Read CSV in 200,000-row batches via batched reader
#   3) Final fallback: direct read_csv()
#
# Logging: Centralized logger ("layer1.reader")
# ---------------------------------------------------------

from pathlib import Path
import polars as pl
import gzip
import tempfile
import os
from util.logger import get_logger

logger = get_logger("layer1.reader")

# GLOBAL SAFE BATCH SIZE (Option B)
SAFE_BATCH_SIZE = 200_000


def _read_csv_in_batches(csv_path: str, batch_size: int):
    """
    Internal helper to read CSV using Polars BatchedCsvReader.
    This works on ALL Polars versions (new & old).
    """

    logger.info(f"Reading CSV in batches of {batch_size:,} rows...")

    reader = pl.read_csv_batched(
        csv_path,
        batch_size=batch_size,
        has_header=True,
        ignore_errors=True,
    )

    batches = []
    batch_index = 0
    total_rows = 0

    while True:
        batch_list = reader.next_batches(1)  # returns [] or [DataFrame]

        if not batch_list:
            break

        batch = batch_list[0]
        batch_index += 1
        total_rows += batch.height

        logger.debug(f"  Batch {batch_index}: {batch.height:,} rows")
        batches.append(batch)

    logger.info(f"Batch reading finished → {batch_index} batches, {total_rows:,} rows")

    if not batches:
        raise RuntimeError("No data batches returned from CSV reader")

    return pl.concat(batches, how="vertical")


def read_raw_with_polars(raw_path: Path, infer_rows: int = 50_000) -> pl.DataFrame:
    """
    Safest cross-platform CSV.GZ reader for Windows/Linux.

    Steps:
        1) Decompress to temp CSV (chunked)
        2) Read CSV using batched reader (200k chunks)
        3) Fallback to direct read_csv()

    Returns:
        Polars DataFrame
    """

    raw_path = Path(raw_path)
    logger.info(f"Reading raw CSV.GZ with Polars: {raw_path}")
    logger.info(f"Compressed size = {raw_path.stat().st_size / (1024**2):.2f} MB")

    temp_csv = None

    # ---------------------------------------------------------
    # TRY 1 — Decompress CSV.GZ → temporary CSV file (chunked)
    # ---------------------------------------------------------
    try:
        logger.info("Attempt 1: Decompressing to temp file (1MB chunks)...")

        with tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv", delete=False) as tmp:
            temp_csv = tmp.name
            logger.info(f"→ Temp file: {temp_csv}")

            with gzip.open(str(raw_path), "rb") as gz:
                chunk_size = 1024 * 1024  # 1MB
                while True:
                    chunk = gz.read(chunk_size)
                    if not chunk:
                        break
                    tmp.write(chunk)

        logger.info(
            f"Decompressed CSV size = {Path(temp_csv).stat().st_size / (1024**2):.2f} MB"
        )

        # Read CSV in batches (200k rows)
        df = _read_csv_in_batches(temp_csv, SAFE_BATCH_SIZE)
        logger.info(
            f"Temp decompress SUCCESS → rows={df.height:,}, cols={len(df.columns)}"
        )

        return df

    except Exception as e:
        logger.warning(
            f"Temp decompress/batch-read failed → {str(e)[:150]}. Trying direct batch read..."
        )
    finally:
        if temp_csv and os.path.exists(temp_csv):
            try:
                os.remove(temp_csv)
                logger.debug(f"Removed temp file: {temp_csv}")
            except Exception as cleanup_err:
                logger.warning(f"Could not delete temp file: {cleanup_err}")

    # ---------------------------------------------------------
    # TRY 2 — Direct batched read WITHOUT decompressing first
    # ---------------------------------------------------------
    try:
        logger.info("Attempt 2: Direct batched read_csv_batched()...")
        df = _read_csv_in_batches(str(raw_path), SAFE_BATCH_SIZE)
        logger.info(
            f"Direct batched read SUCCESS → rows={df.height:,}, cols={len(df.columns)}"
        )
        return df

    except Exception as e:
        logger.warning(
            f"Direct batched read failed → {str(e)[:150]}. Trying standard read_csv..."
        )

    # ---------------------------------------------------------
    # TRY 3 — Last fallback: Standard read_csv()
    # ---------------------------------------------------------
    try:
        logger.info("Attempt 3: Standard read_csv() (slow but reliable)...")

        df_fallback = pl.read_csv(
            str(raw_path),
            ignore_errors=True,
            low_memory=True,
        )

        if not isinstance(df_fallback, pl.DataFrame):
            raise TypeError(f"read_csv returned: {type(df_fallback)}")

        logger.info(
            f"Standard read SUCCESS → rows={df_fallback.height:,}, cols={len(df_fallback.columns)}"
        )
        return df_fallback

    except Exception as fatal:
        logger.error(f"❌ All read attempts FAILED → {str(fatal)[:200]}")
        raise RuntimeError(f"Unable to read CSV.GZ file: {raw_path}") from fatal
