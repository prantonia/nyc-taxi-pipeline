#!/usr/bin/env python3
"""
Utility script to create all BigQuery tables.

Usage:
    python src/create_tables.py
"""
import sys
import logging
from pathlib import Path
from src.orchestrator import PipelineOrchestrator
from src.config import LOG_LEVEL, LOG_FORMAT

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def main():
    """Create all BigQuery tables."""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    logger = logging.getLogger(__name__)
    logger.info("Creating BigQuery tables...")

    try:
        orchestrator = PipelineOrchestrator()
        orchestrator.create_tables()
        orchestrator.close()

        logger.info("All tables created successfully")
        logger.info("=" * 50)
        logger.info("Tables created:")
        logger.info("  - pipeline_metadata")
        logger.info("  - staging_yellow_taxi")
        logger.info("  - raw_yellow_taxi")
        logger.info("=" * 50)
        logger.info(
            "Note: Silver and Gold tables will be created on first pipeline run"
        )

    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
