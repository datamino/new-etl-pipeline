# pipeline.py
# ---------------------------------------------------------
# CLI entrypoint for running the ETL pipeline.
#
# Responsibilities:
#   - Parse CLI arguments
#   - Validate date
#   - Convert YYYYMMDD → YYYY-MM-DD
#   - Trigger main Prefect flow
#
# Logging:
#   Centralized 
#   Module logger name: pipeline
# ---------------------------------------------------------

import sys
from datetime import datetime
from pathlib import Path

from util.logger import get_logger
from flows.main_pipeline import main_pipeline_flow

logger = get_logger("pipeline")


def main():
    logger.info("Starting ETL pipeline CLI entrypoint")

    # -----------------------------------------------------
    # Validate CLI arguments
    # -----------------------------------------------------
    if len(sys.argv) != 2:
        logger.error("Invalid usage. Expected format: python pipeline.py YYYYMMDD")
        sys.exit(1)

    date_arg = sys.argv[1]
    logger.info(f"Received date argument: {date_arg}")

    # -----------------------------------------------------
    # Validate date format (YYYYMMDD)
    # -----------------------------------------------------
    try:
        datetime.strptime(date_arg, "%Y%m%d")
    except ValueError:
        logger.error(f"Invalid date format: {date_arg}. Expected YYYYMMDD.")
        sys.exit(1)

    # Convert to YYYY-MM-DD
    formatted_date = f"{date_arg[:4]}-{date_arg[4:6]}-{date_arg[6:8]}"
    logger.info(f"Formatted execution date: {formatted_date}")

    # -----------------------------------------------------
    # Execute main ETL flow (Prefect)
    # -----------------------------------------------------
    logger.info("Triggering main Prefect ETL pipeline flow")
    main_pipeline_flow(formatted_date)

    logger.info("ETL pipeline CLI execution completed successfully")


if __name__ == "__main__":
    main()
