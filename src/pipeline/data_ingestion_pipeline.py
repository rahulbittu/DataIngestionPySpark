"""
Data ingestion pipeline for processing data from various sources,
classifying it, and passing it to the transform layer.
"""

import os
import time
import logging
from typing import Dict, Any, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp

from src.connectors import (
    FileConnector,
    DatabaseConnector,
    APIConnector,
    KafkaConnector
)
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
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the data ingestion pipeline.
        
        Args:
            spark: The SparkSession
            config_loader: Configuration loader
            logger: Logger instance
        """
        self.spark = spark
        self.config_loader = config_loader
        self.logger = logger or logging.getLogger(__name__)
        
        # Get classification thresholds
        self.classification_thresholds = config_loader.get_classification_thresholds()
        
        # Get target paths
        self.target_paths = config_loader.get_target_paths()
        
        # Create connector registry
        self.connectors = {}
        
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
            
            # Save classified data
            if not self._save_classified_data(df_with_metadata, classification, source_name):
                self.logger.error(f"Failed to save classified data for {source_name}")
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
    
    def run_pipeline(self) -> Dict[str, Any]:
        """
        Run the data ingestion pipeline.
        
        Returns:
            Dict[str, Any]: Pipeline metrics
        """
        self.logger.info("Starting data ingestion pipeline")
        
        try:
            # Process all sources
            metrics = self.process_all_sources()
            
            self.logger.info("Data ingestion pipeline completed successfully")
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error running data ingestion pipeline: {str(e)}")
            raise
