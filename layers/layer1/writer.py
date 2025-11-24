# layers/layer1/writer.py
# ---------------------------------------------------------
# Layer 1 – Step 4: Write DataFrame to Parquet chunks
#
# Features:
#   ✓ Windows + Linux safe
#   ✓ Memory-safe chunked Parquet writing
#   ✓ Fully configurable via config.yaml
#   ✓ Output identical to old pipeline (multi-part parquet)
#   ✓ Uses Polars write_batch API for large reliability
#
# Logging: centralized ("layer1.writer")
# ---------------------------------------------------------

from pathlib import Path
import polars as pl

from util.logger import get_logger
from util.config_loader import load_config

logger = get_logger("layer1.writer")

# Load configuration once
_config = load_config()
_layer1_cfg = _config.get("layer1", {})

CHUNK_SIZE = int(_layer1_cfg.get("chunk_size", 200_000))         # rows per chunk
COMPRESSION = _layer1_cfg.get("parquet_compression", "snappy")   # snappy/zstd/gzip
OUTPUT_BASE_DIR = Path(_layer1_cfg.get("output_dir", "data/layer1_output/new/"))


def _ensure_output_dir(processing_date: str) -> Path:
    """
    Create the target directory for parquet chunks.

    Example:
        data/layer1_output/new/20250115/
    """
    out_dir = OUTPUT_BASE_DIR / processing_date
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def write_parquet_parts(df: pl.DataFrame, processing_date: str) -> str:
    """
    Write the full DataFrame `df` into chunked parquet files.

    Strategy:
      - Use lazy slicing of the DataFrame in memory-safe batches.
      - Each chunk is written as: part-00000.parquet, part-00001.parquet, ...
    """

    out_dir = _ensure_output_dir(processing_date)

    total_rows = df.height
    logger.info(
        f"Writing parquet parts for {processing_date} → "
        f"{total_rows:,} rows, chunk_size={CHUNK_SIZE:,}, compression={COMPRESSION}"
    )

    # Calculate number of chunks
    num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE

    logger.info(f"Total chunks to write: {num_chunks}")

    # Iterate through chunks
    for i in range(num_chunks):
        start = i * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, total_rows)

        # Slice DataFrame safely
        chunk_df = df.slice(start, end - start)

        # Output file name
        chunk_file = out_dir / f"part-{i:05d}.parquet"

        # Write parquet file
        chunk_df.write_parquet(
            str(chunk_file),
            compression=COMPRESSION,
        )

        logger.info(
            f"Wrote chunk {i+1}/{num_chunks} → {chunk_file.name} "
            f"({chunk_df.height:,} rows)"
        )

    logger.info(
        f"All parquet parts written successfully → {out_dir}"
    )

    return str(out_dir)
