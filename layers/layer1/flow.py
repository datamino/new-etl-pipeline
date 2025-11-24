# layers/layer1/flow.py
# ---------------------------------------------------------
# Layer 1 – Prefect Orchestration for NEW Cars processing.
# Includes:
#   ✔ Professional retry logic for heavy I/O tasks
#   ✔ Full end-to-end performance logging
#   ✔ Schema validation after writing parquet
#   ✔ Centralized logger usage
# ---------------------------------------------------------

from pathlib import Path
from prefect import flow, task
import time

from util.logger import get_logger
from .file_locator import locate_raw_file
from .reader import read_raw_with_polars
from .normalizer import normalize_dataframe_to_schema, load_schema_from_config
from .writer import write_parquet_parts
from .validator import validate_output_schema    # ⬅ NEW IMPORT

# Module logger
logger = get_logger("layer1.flow")


# ---------------------------------------------------------
# Prefect Tasks
# ---------------------------------------------------------

@task(name="Layer1 ▸ Locate Raw File")
def task_locate_file(processing_date: str) -> Path:
    start = time.time()
    logger.info(f"[Task] Locating raw file for date: {processing_date}")

    path = locate_raw_file(processing_date)

    duration = time.time() - start
    logger.info(f"[PERF] Locate Raw File completed in {duration:.2f} sec")

    return path


@task(
    name="Layer1 ▸ Read CSV.GZ",
    retries=3,
    retry_delay_seconds=5
)
def task_read(raw_path: Path):
    start = time.time()
    logger.info(f"[Task] Reading raw CSV.GZ: {raw_path}")

    df = read_raw_with_polars(raw_path)

    duration = time.time() - start
    logger.info(
        f"[PERF] Read CSV.GZ completed in {duration:.2f} sec "
        f"(rows={df.height:,}, cols={len(df.columns)})"
    )

    return df


@task(name="Layer1 ▸ Normalize Schema")
def task_normalize(df):
    start = time.time()
    logger.info("[Task] Normalizing DataFrame to FULL_COLUMNS_NEW schema")

    schema = load_schema_from_config()
    df_norm = normalize_dataframe_to_schema(df, schema)

    duration = time.time() - start
    logger.info(
        f"[PERF] Normalize Schema completed in {duration:.2f} sec "
        f"(rows={df_norm.height:,})"
    )

    return df_norm


@task(
    name="Layer1 ▸ Write Parquet Parts",
    retries=3,
    retry_delay_seconds=5
)
def task_write(df, processing_date: str):
    start = time.time()
    logger.info(f"[Task] Writing parquet parts for {processing_date}")

    out_dir = write_parquet_parts(df, processing_date)

    duration = time.time() - start
    logger.info(f"[PERF] Write Parquet Parts completed in {duration:.2f} sec")

    return out_dir


# ---------------------------------------------------------
# NEW — Schema Validator Task
# ---------------------------------------------------------

@task(name="Layer1 ▸ Validate Parquet Schema")
def task_validate_output(out_dir: str):
    """
    Validate schema by reading first parquet part and comparing
    with FULL_COLUMNS_NEW from config.
    """
    start = time.time()
    logger.info(f"[Task] Validating output schema in: {out_dir}")

    ok = validate_output_schema(Path(out_dir))

    duration = time.time() - start
    if ok:
        logger.info(f"[PERF] Schema validation completed in {duration:.2f} sec")
        return True
    else:
        raise RuntimeError("❌ Schema validation failed — stopping pipeline")


# ---------------------------------------------------------
# Prefect Master Flow
# ---------------------------------------------------------

@flow(name="Layer 1 – Prepare NEW Cars Parquet Dataset (modular)")
def layer1_flow(processing_date: str):
    logger.info(f"🚀 Starting Layer1 Flow for date: {processing_date}")
    total_start = time.time()

    # Step 1: Locate source file
    raw_path = task_locate_file(processing_date)

    # Step 2: Read CSV.GZ
    df = task_read(raw_path)

    # Step 3: Normalize schema
    normalized_df = task_normalize(df)

    # Step 4: Write parquet chunks
    out_dir = task_write(normalized_df, processing_date)

    # Step 5: Validate parquet schema  ⬅ NEW
    task_validate_output(out_dir)

    total_duration = time.time() - total_start
    logger.info(f"⏱️ [Layer1 TOTAL] Completed in {total_duration:.2f} sec")
    logger.info(f"✅ Layer1 Flow Completed Successfully → Output: {out_dir}")

    return out_dir
