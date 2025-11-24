"""
Generate a dummy MarketCheck NEW dataset matching FULL_COLUMNS_NEW schema.
Creates:
    - test_input/mc_us_new_combined_YYYYMMDD.csv
    - test_input/mc_us_new_combined_YYYYMMDD.csv.gz
"""

import os
import gzip
import pandas as pd
from pathlib import Path
from datetime import datetime
from util.config_loader import load_config


def generate_dummy_dataset(processing_date: str, rows: int = 100_000):
    """
    Generate a dummy dataset for testing Layer-1 pipeline.
    """
    config = load_config()
    columns = config["layer1"]["full_columns_new"]
    
    out_dir = Path("test_input")
    out_dir.mkdir(exist_ok=True)
    
    csv_path = out_dir / f"mc_us_new_combined_{processing_date}.csv"
    gz_path  = out_dir / f"mc_us_new_combined_{processing_date}.csv.gz"

    print(f"Generating dummy dataset with {rows:,} rows...")
    print(f"Columns: {len(columns)} columns")

    # Create dummy data
    data = {col: [f"{col}_value"] * rows for col in columns}

    df = pd.DataFrame(data)

    # Save CSV
    df.to_csv(csv_path, index=False)
    print(f"CSV saved → {csv_path}")

    # Compress to .gz
    with open(csv_path, "rb") as f_in:
        with gzip.open(gz_path, "wb") as f_out:
            f_out.writelines(f_in)

    print(f"GZIP saved → {gz_path}")
    print("Done!")


if __name__ == "__main__":
    # Example: python gen_dummy_dataset.py 20250115
    processing_date = datetime.now().strftime("%Y%m%d")
    generate_dummy_dataset(processing_date, rows=10)
