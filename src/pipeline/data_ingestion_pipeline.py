"""
Data ingestion pipeline for processing data from various sources,
classifying it, and passing it to the transform layer.
Supports both batch and real-time streaming processing.
"""

import os
import time
import logging
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Callable, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp, window, col
from pyspark.sql.streaming import StreamingQuery

from src.connectors import (
    FileConnector,
    DatabaseConnector,
    APIConnector,
    KafkaConnector
)
from src.connectors.elasticsearch_connector import ElasticsearchConnector
from src.classifiers import DataClassifier
from src.utils.config_loader import ConfigLoader
from src.utils.logging_utils import log_dataframe_info, log_metrics


class DataIngestionPipeline:
    """
    Pipeline for ingesting data from various sources, 
    classifying it, and passing it to the transform layer.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config_loader: ConfigLoader,
        logger: Optional[logging.Logger] = None,
        schema_registry = None,
        use_elasticsearch: bool = False
    ):
        """
        Initialize the data ingestion pipeline.
        
        Args:
            spark: The SparkSession
            config_loader: Configuration loader
            logger: Logger instance
            schema_registry: Optional schema registry for schema validation and evolution
            use_elasticsearch: Whether to use Elasticsearch for storing data and metrics
        """
        self.spark = spark
        self.config_loader = config_loader
        self.logger = logger or logging.getLogger(__name__)
        self.schema_registry = schema_registry
        self.use_elasticsearch = use_elasticsearch
        
        # Get classification thresholds
        self.classification_thresholds = config_loader.get_classification_thresholds()
        
        # Get target paths
        self.target_paths = config_loader.get_target_paths()
        
        # Create connector registry
        self.connectors = {}
        
        # Initialize Elasticsearch connector if enabled
        self.es_connector = None
        if use_elasticsearch:
            self._init_elasticsearch_connector()
        
        # Initialize metrics
        self.pipeline_metrics = {
            'sources_processed': 0,
            'sources_failed': 0,
            'records_processed': 0,
            'bronze_count': 0,
            'silver_count': 0,
            'gold_count': 0,
            'rejected_count': 0,
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0
        }
    
    def _init_elasticsearch_connector(self):
        """
        Initialize the Elasticsearch connector based on configuration.
        """
        try:
            # Get Elasticsearch configuration from the config loader
            es_config = self.config_loader.get_elasticsearch_config()
            
            if not es_config:
                self.logger.warning("Elasticsearch is enabled but no configuration found. Using default values.")
                es_config = {
                    'hosts': ['http://localhost:9200']
                }
                
            # Create the connector
            self.es_connector = ElasticsearchConnector(es_config, self.logger)
            
            # Test connection
            if self.es_connector.connect():
                self.logger.info("Successfully connected to Elasticsearch")
            else:
                self.logger.error("Failed to connect to Elasticsearch. Some features may not work properly.")
                
        except Exception as e:
            self.logger.error(f"Error initializing Elasticsearch connector: {str(e)}")
            self.use_elasticsearch = False
    
    def _save_classified_data_to_elasticsearch(
        self, 
        df: DataFrame, 
        classification: str, 
        source_name: str
    ) -> bool:
        """
        Save classified data to Elasticsearch.
        
        Args:
            df: DataFrame to save
            classification: Classification level (bronze, silver, gold, rejected)
            source_name: Name of the data source
            
        Returns:
            bool: True if save was successful, False otherwise
        """
        if not self.use_elasticsearch or not self.es_connector:
            return False
            
        try:
            # Index name format: {classification}_{source_name}_{date}
            # e.g., gold_customer_data_2025-04-13
            today = datetime.now().strftime('%Y-%m-%d')
            index_name = f"{classification}_{source_name}_{today}"
            
            self.logger.info(f"Indexing {classification} data from {source_name} to Elasticsearch index: {index_name}")
            
            # Index data in Elasticsearch
            success, errors = self.es_connector.index_spark_dataframe(df, index_name)
            
            if success > 0:
                self.logger.info(f"Successfully indexed {success} documents to Elasticsearch index: {index_name}")
                return True
            else:
                self.logger.error(f"Failed to index data to Elasticsearch index: {index_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error indexing data to Elasticsearch: {str(e)}")
            return False
            
    def send_metrics_to_elasticsearch(self, metrics: Dict[str, Any]) -> bool:
        """
        Send metrics to Elasticsearch for visualization in Kibana.
        
        Args:
            metrics: Metrics to send
            
        Returns:
            bool: True if metrics were sent successfully, False otherwise
        """
        if not self.use_elasticsearch or not self.es_connector:
            return False
            
        try:
            return self.es_connector.index_metrics(metrics)
        except Exception as e:
            self.logger.error(f"Error sending metrics to Elasticsearch: {str(e)}")
            return False
    
    def _create_connector(self, source_config: Dict[str, Any], source_type: str):
        """
        Create a connector for the given source configuration.
        
        Args:
            source_config: Source configuration
            source_type: Type of source ('file', 'database', 'api', 'kafka')
            
        Returns:
            BaseConnector: Connector instance
        
        Raises:
            ValueError: If the source type is invalid
        """
        source_name = source_config.get('name')
        
        if source_type == 'file':
            return FileConnector(self.spark, source_config, self.logger)
        elif source_type == 'database':
            return DatabaseConnector(self.spark, source_config, self.logger)
        elif source_type == 'api':
            return APIConnector(self.spark, source_config, self.logger)
        elif source_type == 'kafka':
            return KafkaConnector(self.spark, source_config, self.logger)
        else:
            raise ValueError(f"Invalid source type: {source_type}")
    
    def _save_classified_data(
        self, 
        df: DataFrame, 
        classification: str, 
        source_name: str
    ) -> bool:
        """
        Save classified data to the appropriate target path.
        
        Args:
            df: DataFrame to save
            classification: Classification level (bronze, silver, gold, rejected)
            source_name: Name of the data source
            
        Returns:
            bool: True if save was successful, False otherwise
        """
        if classification not in self.target_paths:
            self.logger.error(f"No target path defined for classification level: {classification}")
            return False
        
        target_base_path = self.target_paths.get(classification)
        target_path = os.path.join(target_base_path, source_name)
        
        try:
            # Save data in parquet format
            self.logger.info(f"Saving {classification} data from {source_name} to {target_path}")
            
            # Add additional metadata
            output_df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("classification_level", lit(classification))
            
            # Write data
            output_df.write \
                .mode("overwrite") \
                .partitionBy("ingestion_timestamp") \
                .parquet(target_path)
            
            self.logger.info(f"Successfully saved {classification} data from {source_name} to {target_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving {classification} data from {source_name}: {str(e)}")
            return False
    
    def process_source(
        self, 
        source_config: Dict[str, Any], 
        source_type: str
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Process a single data source.
        
        Args:
            source_config: Source configuration
            source_type: Type of source ('file', 'database', 'api', 'kafka')
            
        Returns:
            Tuple[bool, Optional[Dict[str, Any]]]: Tuple of (success, metrics)
        """
        source_name = source_config.get('name', 'unnamed_source')
        self.logger.info(f"Processing {source_type} source: {source_name}")
        
        try:
            # Create connector
            connector = self._create_connector(source_config, source_type)
            
            # Connect to source
            if not connector.connect():
                self.logger.error(f"Failed to connect to {source_type} source: {source_name}")
                return False, None
            
            # Read data
            self.logger.info(f"Reading data from {source_type} source: {source_name}")
            df = connector.read_data()
            
            # Log DataFrame info
            log_dataframe_info(self.logger, df, source_name)
            
            # Create classifier
            classifier = DataClassifier(
                self.spark, 
                source_config, 
                self.classification_thresholds, 
                self.logger
            )
            
            # Classify data
            self.logger.info(f"Classifying data from {source_name}")
            classification, metrics = classifier.classify_data(df)
            
            # Log metrics
            log_metrics(self.logger, metrics, source_name)
            
            # Add classification metadata
            df_with_metadata = classifier.add_classification_metadata(df, classification, metrics)
            
            # Save classified data to file system
            file_save_success = self._save_classified_data(df_with_metadata, classification, source_name)
            
            # Save to Elasticsearch if enabled
            es_save_success = True
            if self.use_elasticsearch:
                es_save_success = self._save_classified_data_to_elasticsearch(df_with_metadata, classification, source_name)
                
            # If both saves failed, return error
            if not file_save_success and not es_save_success:
                self.logger.error(f"Failed to save classified data for {source_name} to any destination")
                return False, metrics
            
            # Update metrics
            record_count = df.count()
            
            # Return success and metrics
            result_metrics = {
                'source_name': source_name,
                'source_type': source_type,
                'record_count': record_count,
                'classification': classification,
                'quality_metrics': metrics
            }
            
            self.logger.info(f"Successfully processed {source_type} source: {source_name}")
            return True, result_metrics
            
        except Exception as e:
            self.logger.error(f"Error processing {source_type} source {source_name}: {str(e)}")
            return False, None
    
    def process_all_sources(self) -> Dict[str, Any]:
        """
        Process all configured data sources.
        
        Returns:
            Dict[str, Any]: Pipeline metrics
        """
        # Reset metrics
        self.pipeline_metrics = {
            'sources_processed': 0,
            'sources_failed': 0,
            'records_processed': 0,
            'bronze_count': 0,
            'silver_count': 0,
            'gold_count': 0,
            'rejected_count': 0,
            'start_time': time.time(),
            'end_time': None,
            'duration_seconds': 0
        }
        
        # Get all sources
        all_sources = self.config_loader.get_all_sources()
        
        # Process each source type
        source_metrics = []
        
        # Process file sources
        for source_config in all_sources.get('file_sources', []):
            success, metrics = self.process_source(source_config, 'file')
            if success:
                self.pipeline_metrics['sources_processed'] += 1
                source_metrics.append(metrics)
                
                # Update record counts
                record_count = metrics.get('record_count', 0)
                self.pipeline_metrics['records_processed'] += record_count
                
                # Update classification counts
                classification = metrics.get('classification')
                if classification == 'bronze':
                    self.pipeline_metrics['bronze_count'] += record_count
                elif classification == 'silver':
                    self.pipeline_metrics['silver_count'] += record_count
                elif classification == 'gold':
                    self.pipeline_metrics['gold_count'] += record_count
                elif classification == 'rejected':
                    self.pipeline_metrics['rejected_count'] += record_count
            else:
                self.pipeline_metrics['sources_failed'] += 1
        
        # Process database sources
        for source_config in all_sources.get('database_sources', []):
            success, metrics = self.process_source(source_config, 'database')
            if success:
                self.pipeline_metrics['sources_processed'] += 1
                source_metrics.append(metrics)
                
                # Update record counts
                record_count = metrics.get('record_count', 0)
                self.pipeline_metrics['records_processed'] += record_count
                
                # Update classification counts
                classification = metrics.get('classification')
                if classification == 'bronze':
                    self.pipeline_metrics['bronze_count'] += record_count
                elif classification == 'silver':
                    self.pipeline_metrics['silver_count'] += record_count
                elif classification == 'gold':
                    self.pipeline_metrics['gold_count'] += record_count
                elif classification == 'rejected':
                    self.pipeline_metrics['rejected_count'] += record_count
            else:
                self.pipeline_metrics['sources_failed'] += 1
        
        # Process API sources
        for source_config in all_sources.get('api_sources', []):
            success, metrics = self.process_source(source_config, 'api')
            if success:
                self.pipeline_metrics['sources_processed'] += 1
                source_metrics.append(metrics)
                
                # Update record counts
                record_count = metrics.get('record_count', 0)
                self.pipeline_metrics['records_processed'] += record_count
                
                # Update classification counts
                classification = metrics.get('classification')
                if classification == 'bronze':
                    self.pipeline_metrics['bronze_count'] += record_count
                elif classification == 'silver':
                    self.pipeline_metrics['silver_count'] += record_count
                elif classification == 'gold':
                    self.pipeline_metrics['gold_count'] += record_count
                elif classification == 'rejected':
                    self.pipeline_metrics['rejected_count'] += record_count
            else:
                self.pipeline_metrics['sources_failed'] += 1
        
        # Process Kafka sources
        for source_config in all_sources.get('kafka_sources', []):
            success, metrics = self.process_source(source_config, 'kafka')
            if success:
                self.pipeline_metrics['sources_processed'] += 1
                source_metrics.append(metrics)
                
                # For streaming sources, we don't have accurate record counts
                # So we don't update the record counts here
            else:
                self.pipeline_metrics['sources_failed'] += 1
        
        # Update end time and duration
        self.pipeline_metrics['end_time'] = time.time()
        self.pipeline_metrics['duration_seconds'] = self.pipeline_metrics['end_time'] - self.pipeline_metrics['start_time']
        
        # Log pipeline metrics
        self.logger.info(f"Pipeline metrics: {self.pipeline_metrics}")
        
        # Return metrics
        return {
            'pipeline_metrics': self.pipeline_metrics,
            'source_metrics': source_metrics
        }
    
    def _save_streaming_classified_data(
        self,
        streaming_df: DataFrame,
        classification: str,
        source_name: str,
        checkpoint_dir: str = "./checkpoints",
        query_name: Optional[str] = None,
        trigger_interval: Optional[str] = None
    ) -> StreamingQuery:
        """
        Save classified streaming data to the appropriate target path.
        
        Args:
            streaming_df: Streaming DataFrame to save
            classification: Classification level (bronze, silver, gold, rejected)
            source_name: Name of the data source
            checkpoint_dir: Directory for checkpoints
            query_name: Optional name for the streaming query
            trigger_interval: Optional trigger interval (e.g., "10 seconds")
            
        Returns:
            StreamingQuery: The streaming query
            
        Raises:
            ValueError: If there's no target path for the classification level
        """
        if classification not in self.target_paths:
            raise ValueError(f"No target path defined for classification level: {classification}")
        
        target_base_path = self.target_paths.get(classification)
        target_path = os.path.join(target_base_path, source_name)
        
        # Add additional metadata
        output_df = streaming_df.withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("classification_level", lit(classification))
        
        # Create query name if not provided
        actual_query_name = query_name or f"save-{classification}-data-{source_name}"
        checkpoint_location = os.path.join(checkpoint_dir, actual_query_name)
        
        # Create the streaming writer
        writer = output_df.writeStream \
            .format("parquet") \
            .option("path", target_path) \
            .option("checkpointLocation", checkpoint_location) \
            .partitionBy("ingestion_timestamp") \
            .queryName(actual_query_name)
        
        # Add trigger if specified
        if trigger_interval:
            writer = writer.trigger(processingTime=trigger_interval)
        
        # Start the streaming query
        self.logger.info(f"Starting streaming query to save {classification} data from {source_name} to {target_path}")
        query = writer.start()
        
        return query
    
    def process_streaming_source(
        self,
        source_config: Dict[str, Any],
        source_type: str,
        checkpoint_dir: str = "./checkpoints",
        trigger_interval: Optional[str] = None,
        window_duration: Optional[str] = None,
        sliding_duration: Optional[str] = None
    ) -> Optional[Dict[str, StreamingQuery]]:
        """
        Process a streaming data source.
        
        Args:
            source_config: Source configuration
            source_type: Type of source ('kafka')
            checkpoint_dir: Directory for checkpoints
            trigger_interval: Optional trigger interval
            window_duration: Optional window duration for windowed operations
            sliding_duration: Optional sliding duration for windowed operations
            
        Returns:
            Optional[Dict[str, StreamingQuery]]: Dictionary of streaming queries or None if failed
        """
        source_name = source_config.get('name', 'unnamed_source')
        self.logger.info(f"Processing streaming {source_type} source: {source_name}")
        
        try:
            # Only Kafka is supported for streaming currently
            if source_type != 'kafka':
                self.logger.error(f"Streaming not supported for source type: {source_type}")
                return None
            
            # Create connector
            connector = self._create_connector(source_config, source_type)
            
            # Connect to source
            if not connector.connect():
                self.logger.error(f"Failed to connect to {source_type} source: {source_name}")
                return None
            
            # Create classifier
            classifier = DataClassifier(
                self.spark, 
                source_config, 
                self.classification_thresholds, 
                self.logger
            )
            
            # Define the streaming processing function
            def process_streaming_data(stream_df: DataFrame) -> DataFrame:
                """Process streaming data through classification pipeline"""
                
                # Apply windowing if specified
                if window_duration:
                    # Assume there's a timestamp column or add processing time
                    if "_event_time" in stream_df.columns:
                        timestamp_col = "_event_time"
                    else:
                        stream_df = stream_df.withColumn("_processing_time", current_timestamp())
                        timestamp_col = "_processing_time"
                    
                    # Apply windowing
                    if sliding_duration:
                        stream_df = stream_df.withWatermark(timestamp_col, window_duration) \
                            .groupBy(window(col(timestamp_col), window_duration, sliding_duration))
                    else:
                        stream_df = stream_df.withWatermark(timestamp_col, window_duration) \
                            .groupBy(window(col(timestamp_col), window_duration))
                
                # Apply schema validation (but not full classification as it requires count operations)
                # This is a simplified version for streaming
                is_valid, _ = classifier.validate_schema(stream_df)
                
                # Add validation metadata
                result_df = stream_df.withColumn("_schema_valid", lit(is_valid))
                
                # We can't do full classification in streaming mode due to stateless operation
                # requirements, but we can add some basic quality indicators
                result_df = result_df.withColumn("_quality_check_time", current_timestamp())
                
                # Return processed DataFrame
                return result_df
            
            # Start the streaming query
            streaming_query = connector.start_streaming(
                process_func=process_streaming_data,
                output_mode="append",
                checkpoint_location=checkpoint_dir,
                query_name=f"stream-{source_name}",
                trigger_interval=trigger_interval,
                output_sink="memory"  # Start with memory sink for initial processing
            )
            
            self.logger.info(f"Successfully started streaming query for {source_type} source: {source_name}")
            
            # Return the streaming query
            return {
                'main_query': streaming_query
            }
            
        except Exception as e:
            self.logger.error(f"Error processing streaming {source_type} source {source_name}: {str(e)}")
            return None
    
    def start_streaming_pipeline(
        self, 
        checkpoint_dir: str = "./checkpoints",
        trigger_interval: str = "10 seconds",
        window_duration: Optional[str] = None,
        sliding_duration: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Start the streaming data ingestion pipeline.
        
        Args:
            checkpoint_dir: Directory for checkpoints
            trigger_interval: Trigger interval (e.g., "10 seconds")
            window_duration: Optional window duration for windowed operations
            sliding_duration: Optional sliding duration for windowed operations
            
        Returns:
            Dict[str, Any]: Dictionary of streaming queries
        """
        self.logger.info("Starting streaming data ingestion pipeline")
        
        # Get all sources
        all_sources = self.config_loader.get_all_sources()
        
        # Process streaming sources (only Kafka for now)
        streaming_queries = {}
        
        # Process Kafka sources
        for source_config in all_sources.get('kafka_sources', []):
            source_name = source_config.get('name', 'unnamed_kafka_source')
            
            # Skip non-streaming sources
            if not source_config.get('streaming_enabled', False):
                self.logger.info(f"Skipping non-streaming Kafka source: {source_name}")
                continue
            
            # Process streaming source
            queries = self.process_streaming_source(
                source_config=source_config,
                source_type='kafka',
                checkpoint_dir=checkpoint_dir,
                trigger_interval=trigger_interval,
                window_duration=window_duration,
                sliding_duration=sliding_duration
            )
            
            if queries:
                streaming_queries[source_name] = queries
                self.logger.info(f"Started streaming for Kafka source: {source_name}")
            else:
                self.logger.error(f"Failed to start streaming for Kafka source: {source_name}")
        
        self.logger.info(f"Streaming pipeline started with {len(streaming_queries)} sources")
        
        # Send metrics to Elasticsearch if enabled
        if self.use_elasticsearch:
            self.logger.info("Sending streaming pipeline metrics to Elasticsearch")
            
            # Create streaming metrics for Elasticsearch
            es_metrics = {
                "pipeline_run": {
                    "timestamp": datetime.now().isoformat(),
                    "mode": "streaming",
                    "streaming_sources": len(streaming_queries),
                    "streaming_queries": [
                        {
                            "source_name": source_name,
                            "query_names": list(queries.keys()) if queries else []
                        }
                        for source_name, queries in streaming_queries.items()
                    ]
                }
            }
            
            # Send to Elasticsearch
            if self.send_metrics_to_elasticsearch(es_metrics):
                self.logger.info("Successfully sent streaming metrics to Elasticsearch")
            else:
                self.logger.warning("Failed to send streaming metrics to Elasticsearch")
                
        return streaming_queries
    
    def process_streaming_events(
        self,
        source_config: Dict[str, Any],
        event_processor: Callable[[Dict[str, Any]], None],
        batch_mode: bool = False,
        batch_size: int = 100,
        run_in_thread: bool = True
    ) -> Optional[threading.Thread]:
        """
        Process streaming events directly with a callback function.
        This is an alternative to Spark Structured Streaming for lower-latency processing.
        
        Args:
            source_config: Source configuration
            event_processor: Function to process each event
            batch_mode: Whether to process events in batches
            batch_size: Batch size when in batch mode
            run_in_thread: Whether to run in a separate thread
            
        Returns:
            Optional[threading.Thread]: Thread if running in thread mode, None otherwise
        """
        source_name = source_config.get('name', 'unnamed_source')
        
        try:
            # Only Kafka is supported for streaming currently
            connector = self._create_connector(source_config, 'kafka')
            
            # Connect to source
            if not connector.connect():
                self.logger.error(f"Failed to connect to Kafka source: {source_name}")
                return None
            
            # Start event stream
            result = connector.start_event_stream(
                consumer_name=f"event-consumer-{source_name}",
                process_func=event_processor,
                batch_mode=batch_mode,
                batch_size=batch_size,
                run_in_thread=run_in_thread
            )
            
            self.logger.info(f"Started event stream for Kafka source: {source_name}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error starting event stream for {source_name}: {str(e)}")
            return None
    
    def run_pipeline(self) -> Dict[str, Any]:
        """
        Run the data ingestion pipeline in batch mode.
        
        Returns:
            Dict[str, Any]: Pipeline metrics
        """
        self.logger.info("Starting data ingestion pipeline (batch mode)")
        
        try:
            # Process all sources
            metrics = self.process_all_sources()
            
            # Send metrics to Elasticsearch if enabled
            if self.use_elasticsearch:
                self.logger.info("Sending pipeline metrics to Elasticsearch")
                
                # Create a copy of metrics for Elasticsearch with timestamp
                es_metrics = {
                    "pipeline_run": {
                        "timestamp": datetime.now().isoformat(),
                        "mode": "batch",
                        "metrics": metrics.get('pipeline_metrics', {}),
                        "sources": metrics.get('source_metrics', [])
                    }
                }
                
                # Send to Elasticsearch
                if self.send_metrics_to_elasticsearch(es_metrics):
                    self.logger.info("Successfully sent metrics to Elasticsearch")
                else:
                    self.logger.warning("Failed to send metrics to Elasticsearch")
            
            self.logger.info("Data ingestion pipeline completed successfully")
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error running data ingestion pipeline: {str(e)}")
            raise
