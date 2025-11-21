# flows/main_pipeline.py
# ---------------------------------------------------------
# Main Prefect flow that orchestrates all ETL layers.
# For now it runs ONLY Layer 1 (NEW cars).
#
# Logging:
#   Centralized logging (Option C)
#   Module logger name: flows.main_pipeline
# ---------------------------------------------------------

from prefect import flow
from util.logger import get_logger
from layers.layer1.flow import layer1_flow

logger = get_logger("flows.main_pipeline")


@flow(name="Main ETL Pipeline")
def main_pipeline_flow(processing_date: str):
    """
    Main Prefect ETL Orchestration Flow.
    Runs all layers in sequence.
    Currently only Layer 1 is active.
    """
    logger.info(f"Main ETL Pipeline Started for date: {processing_date}")

    # -----------------------
    # Layer 1: NEW Cars
    # -----------------------
    logger.info("▶ Running Layer 1 – NEW Cars Parquet Preparation")
    layer1_flow(processing_date)
    logger.info("✔ Layer 1 completed successfully")

    # -----------------------
    # Future:
    #   Layer 2 (Essential Incremental)
    #   Layer 3 (Sold Detection)
    #   Layer 4 (Dealer Summary)
    #   API & Frontend integration
    # -----------------------

    logger.info("✅ Main ETL Pipeline completed (Layer 1 only for now)")
