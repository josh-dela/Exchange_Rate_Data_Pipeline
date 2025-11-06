"""Entry point for running the ETL pipeline."""
import sys
from src.pipeline.etl_pipeline import ETLPipeline
from src.utils.config import Config
from src.utils.logger import get_logger

logger = get_logger("main")


def main():
    """Main function to run the ETL pipeline."""
    try:
        # Validate configuration
        Config.validate()
        
        logger.info("=" * 60)
        logger.info("Exchange Rate ETL Pipeline - Starting Execution")
        logger.info("=" * 60)
        
        # Initialize and run pipeline
        pipeline = ETLPipeline()
        results = pipeline.run()
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Pipeline Execution Summary")
        logger.info("=" * 60)
        logger.info(f"Status: {'SUCCESS' if results['success'] else 'FAILED'}")
        logger.info(f"Duration: {results.get('duration_seconds', 0):.2f} seconds")
        
        if results['success']:
            logger.info(f"Records Processed: {results.get('records_processed', 0)}")
            
            # Quality metrics
            quality = results.get('quality_metrics', {})
            if quality:
                logger.info(f"Data Completeness: {quality.get('completeness', 0):.2%}")
            
            # Load metrics
            load_metrics = results.get('load_metrics', {})
            if load_metrics:
                logger.info(f"Records Loaded: {load_metrics.get('success_count', 0)}")
                if load_metrics.get('skipped'):
                    logger.info("Note: Supabase not configured, load was skipped")
        else:
            logger.error(f"Error: {results.get('error', 'Unknown error')}")
        
        logger.info("=" * 60)
        
        # Exit with appropriate code
        sys.exit(0 if results['success'] else 1)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

