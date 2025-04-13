"""
Main entry point for the data ingestion and classification system.
This script serves as an entry point for both:
1. The data ingestion pipeline
2. The web dashboard for monitoring
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Import web dashboard app for gunicorn
from web_dashboard.app import app

# Import pipeline components
from src.utils.logging_utils import setup_logging
from src.utils.config_loader import ConfigLoader
from src.utils.spark_session import create_spark_session
from src.pipeline.data_ingestion_pipeline import DataIngestionPipeline


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Data Ingestion and Classification System')
    
    parser.add_argument(
        '--config', 
        type=str, 
        default='./config/data_sources.yml',
        help='Path to the data sources configuration file'
    )
    
    parser.add_argument(
        '--log-config', 
        type=str, 
        default='./config/logging_config.yml',
        help='Path to the logging configuration file'
    )
    
    parser.add_argument(
        '--source', 
        type=str, 
        help='Process only a specific source by name'
    )
    
    parser.add_argument(
        '--verbose', 
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--web-only',
        action='store_true',
        help='Start only the web dashboard, without running the pipeline'
    )
    
    return parser.parse_args()


def run_pipeline():
    """
    Run the data ingestion pipeline.
    """
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = setup_logging(config_path=args.log_config, default_level=log_level)
    
    try:
        # Log startup information
        logger.info("Starting data ingestion and classification system")
        logger.info(f"Using configuration file: {args.config}")
        
        # Create Spark session
        spark = create_spark_session(
            app_name="Data Ingestion and Classification System",
            logger=logger
        )
        
        # Load configuration
        config_loader = ConfigLoader(args.config)
        
        # Create and run pipeline
        pipeline = DataIngestionPipeline(spark, config_loader, logger)
        
        # Run pipeline for all sources or a specific source
        if args.source:
            logger.info(f"Processing only source: {args.source}")
            source_config = config_loader.get_source_by_name(args.source)
            
            if not source_config:
                logger.error(f"Source not found: {args.source}")
                sys.exit(1)
            
            # Determine source type
            source_type = None
            all_sources = config_loader.get_all_sources()
            for type_name, sources in all_sources.items():
                for src in sources:
                    if src.get('name') == args.source:
                        source_type = type_name.replace('_sources', '')
                        break
                if source_type:
                    break
            
            if not source_type:
                logger.error(f"Could not determine source type for: {args.source}")
                sys.exit(1)
            
            # Process single source
            success, metrics = pipeline.process_source(source_config, source_type)
            
            if success:
                logger.info(f"Successfully processed source: {args.source}")
                logger.info(f"Source metrics: {metrics}")
            else:
                logger.error(f"Failed to process source: {args.source}")
                sys.exit(1)
        else:
            # Process all sources
            logger.info("Processing all configured sources")
            metrics = pipeline.run_pipeline()
            
            # Log overall metrics
            pipeline_metrics = metrics.get('pipeline_metrics', {})
            logger.info(f"Pipeline completed. "
                  f"Processed {pipeline_metrics.get('sources_processed', 0)} sources, "
                  f"Failed {pipeline_metrics.get('sources_failed', 0)} sources, "
                  f"Total records: {pipeline_metrics.get('records_processed', 0)}, "
                  f"Duration: {pipeline_metrics.get('duration_seconds', 0):.2f} seconds")
            
            # Log classification distribution
            logger.info(f"Classification distribution: "
                  f"Gold: {pipeline_metrics.get('gold_count', 0)}, "
                  f"Silver: {pipeline_metrics.get('silver_count', 0)}, "
                  f"Bronze: {pipeline_metrics.get('bronze_count', 0)}, "
                  f"Rejected: {pipeline_metrics.get('rejected_count', 0)}")
        
        # Stop Spark session
        spark.stop()
        logger.info("Data ingestion and classification system finished successfully")
        
    except Exception as e:
        logger.exception(f"Error running data ingestion system: {str(e)}")
        sys.exit(1)


def run_web_dashboard():
    """
    Run only the web dashboard.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("web_dashboard")
    logger.info("Starting web dashboard only")
    
    # Get host and port
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', 5000))
    
    # Start the server
    app.run(host=host, port=port, debug=True)


if __name__ == "__main__":
    args = parse_arguments()
    
    if args.web_only:
        run_web_dashboard()
    else:
        run_pipeline()
