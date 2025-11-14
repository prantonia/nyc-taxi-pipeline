#!/usr/bin/env python3
"""
Entry point for Full Refresh Pipeline.
Loads all 2024 NYC Taxi data at once.

Usage:
    python run_full_refresh.py
"""
import sys
import logging
from pathlib import Path


# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.orchestrator import PipelineOrchestrator
from src.config import LOG_FILE, LOG_LEVEL, LOG_FORMAT
from src.retry_handler import RetryHandler

def setup_logging():
    """Configure logging for the pipeline."""
    # Create logs directory if it doesn't exist
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )


def main():
    """Execute full refresh pipeline."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting NYC Taxi Full Refresh Pipeline")
    
    try:
        # Initialize orchestrator
        orchestrator = PipelineOrchestrator()
        
        # Run full refresh
        success = orchestrator.run_full_refresh()
        
        # Close connections
        orchestrator.close()
        
        if success:
            logger.info("Pipeline completed successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()