"""Clean and output data."""

from proctor_schedule.config import PROCESSED_DATA_DIR, RAW_DATA_DIR, LOGS_DIR
from loguru import logger

if __name__ == "__main__":
    logger.add(LOGS_DIR / "dataset.log")