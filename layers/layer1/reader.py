# layers/layer1/reader.py
# ---------------------------------------------------------
# Layer 1 – DuckDB Streaming Reader (Correct / No .offset bug)
# ---------------------------------------------------------

from pathlib import Path
import duckdb
import polars as pl
from util.logger import get_logger

logger = get_logger("layer1.reader")


def read_raw_with_polars(raw_path: Path, batch_size: int = 200_000):
    """
    Stream CSV.GZ using DuckDB + fetch_df_chunk()
    This is the safest, most memory-stable approach for Windows Server.
    """

    raw_path = Path(raw_path)
    logger.info(f"Reading CSV.GZ with DuckDB streaming: {raw_path}")

    try:
        con = duckdb.connect()

        logger.info("DuckDB: Initializing CSV scan using read_csv_auto(...)")

        # Create a DuckDB relation (lazy scan)
        con.execute(f"""
            CREATE TABLE tmp_scan AS 
            SELECT * FROM read_csv_auto('{raw_path}', 
                header=True,
                ignore_errors=True
            );
        """)

        logger.info("DuckDB: Table created. Starting chunked streaming...")

        con.execute("SELECT * FROM tmp_scan")

        df_batches = []
        chunk_idx = 0

        while True:
            chunk = con.fetch_df_chunk(batch_size)
            if chunk is None or chunk.empty:
                break

            chunk_idx += 1
            pl_chunk = pl.from_pandas(chunk)
            df_batches.append(pl_chunk)

            logger.info(f"Chunk {chunk_idx}: {pl_chunk.height} rows")

        if not df_batches:
            raise RuntimeError("DuckDB returned zero chunks. CSV empty?")

        df = pl.concat(df_batches, how="vertical_relaxed")

        logger.info(
            f"DuckDB streaming completed: rows={df.height}, cols={len(df.columns)}"
        )
        return df

    except Exception as e:
        logger.error(f"DuckDB streaming failed: {e}")
        raise
