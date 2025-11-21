# layers/layer1/writer.py
# ---------------------------------------------------------
# Layer 1 – Step 4: Write normalized DataFrame into parquet
# chunk files (part-00000.parquet, part-00001.parquet, ...).
#
# Responsibilities (match old pipeline exactly):
#   - Create output directory: data/layer1_output/new/YYYYMMDD/
#   - Slice into chunks of size = chunk_size from config.yaml
#   - Write parquet files with compression (snappy)
#   - Return output directory Path
#
# Logging:
#   Fully centralized via util.logger (Option C)
#   Logger name: layer1.writer
# ---------------------------------------------------------

from pathlib import Path
import math
import polars as pl

from util.config_loader import load_config
from util.logger import get_logger

# Full module logger name
logger = get_logger("layer1.writer")


def build_output_dir_for_date(base_output: Path, processing_date: str) -> Path:
    """
    Build final output directory for parquet parts:
    Example:
        base_output = "data/layer1_output/new/"
        processing_date = "2025-01-15"
    Result:
        data/layer1_output/new/20250115/
    """
    token = processing_date.replace("-", "")
    out_dir = base_output / token

    out_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created/verified output directory: {out_dir.resolve()}")

    return out_dir


def write_parquet_parts(df: pl.DataFrame, processing_date: str) -> Path:
    """
    Write the normalized DataFrame into chunked parquet files.

    Returns:
        Path to directory containing:
        part-00000.parquet
        part-00001.parquet
        ...
    """
    cfg = load_config()
    layer1_cfg = cfg["layer1"]

    base_output = Path(layer1_cfg["output_dir"])
    chunk_size = int(layer1_cfg.get("chunk_size", 1_000_000))
    compression = layer1_cfg.get("parquet_compression", "snappy")

    out_dir = build_output_dir_for_date(base_output, processing_date)

    total_rows = df.height

    logger.info(
        f"Writing parquet parts for {processing_date} → "
        f"{total_rows:,} rows, chunk_size={chunk_size:,}, compression={compression}"
    )

    # ---------------------------------------------------------
    # Handle empty DataFrame
    # ---------------------------------------------------------
    if total_rows == 0:
        logger.warning(
            f"No rows to write for date {processing_date}. "
            f"Output directory created but contains no parquet files."
        )
        return out_dir

    # ---------------------------------------------------------
    # Compute part count
    # ---------------------------------------------------------
    num_parts = math.ceil(total_rows / chunk_size)
    logger.info(f"Total chunks to write: {num_parts}")

    # ---------------------------------------------------------
    # Write each part
    # ---------------------------------------------------------
    for part_idx in range(num_parts):
        start = part_idx * chunk_size
        length = min(chunk_size, total_rows - start)

        if length <= 0:
            break

        part_df = df.slice(start, length)
        part_filename = f"part-{part_idx:05d}.parquet"
        part_path = out_dir / part_filename

        try:
            part_df.write_parquet(part_path, compression=compression)
            logger.info(
                f"Wrote chunk {part_idx + 1}/{num_parts} → {part_filename} "
                f"({length:,} rows)"
            )
        except Exception as e:
            logger.error(
                f"FAILED writing chunk {part_idx} → {part_filename}: {e}",
                exc_info=True
            )
            raise

    logger.info(f"All parquet parts written successfully → {out_dir.resolve()}")
    return out_dir
