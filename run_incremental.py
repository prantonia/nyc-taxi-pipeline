#!/usr/bin/env python3
"""
Entry point for Incremental Pipeline.
Loads NYC Taxi data month by month.

Usage:
    python run_incremental.py              # Load next month automatically
    python run_incremental.py --month 3    # Load specific month (March)
"""
import sys
import logging
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.orchestrator import PipelineOrchestrator
from src.config import LOG_FILE, LOG_LEVEL, LOG_FORMAT


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


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Run NYC Taxi Incremental Pipeline'
    )
    parser.add_argument(
        '--month',
        type=int,
        choices=range(1, 13),
        help='Specific month to load (1-12). If not provided, loads next month automatically.'
    )
    return parser.parse_args()


def main():
    """Execute incremental pipeline."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    args = parse_args()
    
    if args.month:
        logger.info(f"Starting NYC Taxi Incremental Pipeline for month {args.month}")
    else:
        logger.info("Starting NYC Taxi Incremental Pipeline (automatic month selection)")
    
    try:
        # Initialize orchestrator
        orchestrator = PipelineOrchestrator()
        
        # Run incremental with optional month parameter
        success = orchestrator.run_incremental(target_month=args.month)
        
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