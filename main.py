"""
Main entry point for the data ingestion and classification system.
This script serves as an entry point for both:
1. The data ingestion pipeline (batch and streaming)
2. The web dashboard for real-time monitoring
3. Event-driven schema evolution capabilities
"""

import os
import sys
import argparse
import logging
import threading
import time
import json
from datetime import datetime
from pathlib import Path

# Import web dashboard app for gunicorn
from web_dashboard.app import app, publish_event, update_metrics, update_source_status

# Import pipeline components
from src.utils.logging_utils import setup_logging
from src.utils.config_loader import ConfigLoader
from src.utils.spark_session import create_spark_session
from src.utils.schema_registry import SchemaRegistry
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
    
    parser.add_argument(
        '--streaming',
        action='store_true',
        help='Run in streaming mode instead of batch mode'
    )
    
    parser.add_argument(
        '--trigger-interval',
        type=str,
        default="10 seconds",
        help='Streaming trigger interval (e.g., "10 seconds")'
    )
    
    parser.add_argument(
        '--auto-evolution',
        action='store_true',
        help='Enable auto schema evolution for streaming data'
    )
    
    parser.add_argument(
        '--event-mode',
        action='store_true',
        help='Use event-based processing instead of Spark Structured Streaming'
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
        
        # Check if Elasticsearch configuration exists
        use_elasticsearch = config_loader.get_elasticsearch_config() is not None
        logger.info(f"Elasticsearch integration is {'enabled' if use_elasticsearch else 'disabled'}")
        
        # Create and run pipeline
        pipeline = DataIngestionPipeline(
            spark, 
            config_loader, 
            logger,
            use_elasticsearch=use_elasticsearch
        )
        
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


def handle_schema_event(event_data):
    """
    Handler for schema events.
    
    Args:
        event_data: Schema event data
    """
    logger = logging.getLogger("schema_events")
    event_type = event_data.get('event_type')
    
    # Log the event
    logger.info(f"Schema event received: {event_type}")
    
    # Publish to web dashboard
    publish_event({
        'event_type': 'schema_event',
        'source_name': 'schema_registry',
        'data': event_data
    })
    
    # Handle specific event types
    if event_type == 'schema_evolved':
        # Update metrics
        update_metrics({
            'schema_evolved_count': 1,
            'last_schema_evolution': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })


def process_streaming_event(event_data):
    """
    Process streaming events for real-time dashboard updates.
    
    Args:
        event_data: Event data
    """
    try:
        # Extract event data
        source_name = event_data.get('source_name', 'unknown')
        event_type = event_data.get('event_type', 'data')
        
        # Publish event to dashboard
        publish_event(event_data)
        
        # Update metrics if it's a metrics event
        if event_type == 'metrics':
            metrics_data = event_data.get('metrics', {})
            update_metrics(metrics_data)
        
        # Update source status if it's a status event
        if event_type == 'source_status':
            status_data = event_data.get('status', {})
            update_source_status(source_name, status_data)
            
    except Exception as e:
        logger = logging.getLogger("streaming_events")
        logger.error(f"Error processing streaming event: {e}")


def run_streaming_pipeline():
    """
    Run the data ingestion pipeline in streaming mode.
    """
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = setup_logging(config_path=args.log_config, default_level=log_level)
    
    try:
        # Log startup information
        logger.info("Starting data ingestion system in streaming mode")
        logger.info(f"Using configuration file: {args.config}")
        logger.info(f"Trigger interval: {args.trigger_interval}")
        
        # Create Spark session
        spark = create_spark_session(
            app_name="Data Ingestion Streaming System",
            logger=logger
        )
        
        # Load configuration
        config_loader = ConfigLoader(args.config)
        
        # Initialize schema registry
        schema_registry = SchemaRegistry(
            spark=spark,
            logger=logger,
            auto_evolution=args.auto_evolution,
            watch_interval=30
        )
        
        # Add event listener for schema events
        schema_registry.add_event_listener('schema_updated', handle_schema_event)
        schema_registry.add_event_listener('schema_evolved', handle_schema_event)
        schema_registry.add_event_listener('evolution_requested', handle_schema_event)
        
        # Start schema monitoring if auto-evolution is enabled
        if args.auto_evolution:
            schema_registry.start_schema_monitoring()
            logger.info("Automatic schema evolution monitoring started")
        
        # Check if Elasticsearch configuration exists
        use_elasticsearch = config_loader.get_elasticsearch_config() is not None
        logger.info(f"Elasticsearch integration is {'enabled' if use_elasticsearch else 'disabled'}")
        
        # Create pipeline
        pipeline = DataIngestionPipeline(
            spark, 
            config_loader, 
            logger, 
            schema_registry=schema_registry,
            use_elasticsearch=use_elasticsearch
        )
        
        # Choose processing mode based on arguments
        if args.event_mode:
            # Event-based processing (lower latency, simpler)
            logger.info("Starting event-based streaming pipeline")
            
            # Process streams for each source
            all_sources = config_loader.get_all_sources()
            streaming_threads = []
            
            # Process each Kafka source
            for source_config in all_sources.get('kafka_sources', []):
                if source_config.get('enabled', True):
                    source_name = source_config.get('name', 'unknown')
                    logger.info(f"Starting event stream for source: {source_name}")
                    
                    # Create event processor function
                    def create_event_processor(src_name):
                        def event_processor(event):
                            event['source_name'] = src_name
                            event['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            process_streaming_event(event)
                        return event_processor
                    
                    # Start event stream
                    thread = pipeline.process_streaming_events(
                        source_config,
                        create_event_processor(source_name),
                        batch_mode=False,
                        run_in_thread=True
                    )
                    
                    if thread:
                        streaming_threads.append((source_name, thread))
                        logger.info(f"Event stream started for {source_name}")
            
            # Keep main thread running
            logger.info(f"All event streams started. Total: {len(streaming_threads)}")
            
            try:
                while True:
                    # Check thread health
                    for source_name, thread in streaming_threads:
                        if not thread.is_alive():
                            logger.warning(f"Thread for {source_name} died, restarting")
                            # Restart thread (simplified, would need proper implementation)
                    time.sleep(10)
            except KeyboardInterrupt:
                logger.info("Stopping streaming pipeline")
                if args.auto_evolution:
                    schema_registry.stop_schema_monitoring()
        else:
            # Spark Structured Streaming (higher throughput, more complex)
            logger.info("Starting Spark Structured Streaming pipeline")
            
            # Start streaming pipeline
            streaming_queries = pipeline.start_streaming_pipeline(
                checkpoint_dir="./checkpoints",
                trigger_interval=args.trigger_interval
            )
            
            # Log active queries
            active_queries = {name: query for name, query in streaming_queries.items() if query is not None}
            logger.info(f"Started {len(active_queries)} streaming queries")
            
            # Keep running until interrupted
            try:
                for name, query in active_queries.items():
                    logger.info(f"Query '{name}' running at {query.status['triggerExecution']} ms intervals")
                
                # Wait for termination
                for name, query in active_queries.items():
                    query.awaitTermination()
            except KeyboardInterrupt:
                logger.info("Stopping streaming pipeline")
                
                # Stop all queries
                for name, query in active_queries.items():
                    query.stop()
                    logger.info(f"Stopped query: {name}")
                
                # Stop schema monitoring
                if args.auto_evolution:
                    schema_registry.stop_schema_monitoring()
        
        # Stop Spark session
        spark.stop()
        logger.info("Streaming pipeline stopped")
        
    except Exception as e:
        logger.exception(f"Error running streaming pipeline: {str(e)}")
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
    elif args.streaming:
        run_streaming_pipeline()
    else:
        run_pipeline()
