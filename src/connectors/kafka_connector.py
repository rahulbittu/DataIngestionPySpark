"""
Kafka connector for reading streaming data from Kafka topics.
"""

import os
import logging
from typing import Dict, Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.sql.types import StructType, StringType

from src.connectors.base_connector import BaseConnector


class KafkaConnector(BaseConnector):
    """Connector for reading data from Kafka topics"""
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the Kafka connector.
        
        Args:
            spark: The SparkSession
            source_config: Configuration for the Kafka source
            logger: Logger instance
        """
        super().__init__(spark, source_config, logger)
        
        # Extract Kafka configuration
        self.bootstrap_servers = self._resolve_env_var(source_config.get('bootstrap_servers'))
        self.topic = source_config.get('topic')
        self.group_id = source_config.get('group_id', f'spark-group-{self.name}')
        self.starting_offsets = source_config.get('auto_offset_reset', 'latest')
        
        # Schema for the value (if provided)
        self.value_schema = source_config.get('value_schema')
        self.key_schema = source_config.get('key_schema')
        
        # Streaming configuration
        self.processing = source_config.get('processing', {})
        self.window_duration = self.processing.get('window_duration')
        self.sliding_duration = self.processing.get('sliding_duration')
        
        # Additional Kafka options
        self.kafka_options = source_config.get('kafka_options', {})
    
    def _resolve_env_var(self, value: str) -> str:
        """
        Resolve environment variables in string.
        
        Args:
            value: String that may contain environment variable references like ${VAR_NAME}
            
        Returns:
            str: String with environment variables replaced with their values
        """
        if not value or not isinstance(value, str):
            return value
            
        if value.startswith('${') and value.endswith('}'):
            env_var = value[2:-1]
            return os.environ.get(env_var, '')
        
        return value
    
    def _validate_source_config(self) -> None:
        """
        Validate Kafka source configuration.
        
        Raises:
            ValueError: If any required configuration is missing
        """
        if not self.bootstrap_servers:
            raise ValueError(f"Bootstrap servers must be provided for Kafka source '{self.name}'")
        
        if not self.topic:
            raise ValueError(f"Topic must be provided for Kafka source '{self.name}'")
    
    def connect(self) -> bool:
        """
        Test the Kafka connection.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            # Create a small test DataFrame to check connection
            test_df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.topic) \
                .option("startingOffsets", "latest") \
                .option("kafka.group.id", self.group_id) \
                .load()
            
            # Just get the schema, don't actually execute the query
            _ = test_df.schema
            
            self.logger.info(f"Successfully connected to Kafka: {self.bootstrap_servers}, topic: {self.topic}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error connecting to Kafka '{self.bootstrap_servers}', topic: {self.topic}: {str(e)}")
            return False
    
    def read_data(self) -> DataFrame:
        """
        Read data from Kafka into a Spark DataFrame.
        
        For Kafka, this returns a streaming DataFrame that needs to be processed
        with streaming operations.
        
        Returns:
            DataFrame: The streaming data as a Spark DataFrame
        
        Raises:
            Exception: If there's an error reading from Kafka
        """
        try:
            # Create the base Kafka reader
            reader = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.topic) \
                .option("startingOffsets", self.starting_offsets) \
                .option("kafka.group.id", self.group_id)
            
            # Apply additional Kafka options
            for key, value in self.kafka_options.items():
                reader = reader.option(key, value)
            
            # Load the data
            self.logger.info(f"Reading Kafka stream from topic: {self.topic}")
            kafka_df = reader.load()
            
            # For Kafka, we need to extract and parse key/value
            value_df = kafka_df.selectExpr("CAST(value AS STRING)")
            
            # If a value schema is provided, parse the JSON
            if self.value_schema:
                try:
                    schema = StructType.fromJson(self.value_schema) if isinstance(self.value_schema, dict) else None
                    if schema:
                        value_df = value_df.withColumn("parsed_value", from_json(col("value"), schema)) \
                            .select("parsed_value.*")
                    else:
                        self.logger.warning("Invalid value schema provided, using schema inference")
                        # Infer schema from sample JSON (note: this is less efficient)
                        sample_df = self.spark.read \
                            .format("kafka") \
                            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                            .option("subscribe", self.topic) \
                            .option("startingOffsets", "earliest") \
                            .option("endingOffsets", "latest") \
                            .option("kafka.group.id", f"{self.group_id}-sample") \
                            .load() \
                            .selectExpr("CAST(value AS STRING)") \
                            .limit(10)
                        
                        # Get a sample value to infer schema
                        if sample_df.count() > 0:
                            sample_json = sample_df.first()[0]
                            inferred_schema = schema_of_json(sample_json)
                            value_df = value_df.withColumn("parsed_value", from_json(col("value"), inferred_schema)) \
                                .select("parsed_value.*")
                
                except Exception as schema_err:
                    self.logger.error(f"Error parsing JSON with provided schema: {str(schema_err)}")
                    # Fall back to treating value as string
                    value_df = kafka_df.selectExpr("CAST(value AS STRING) as value")
            
            self.logger.info(f"Successfully created streaming DataFrame from Kafka topic: {self.topic}")
            return value_df
            
        except Exception as e:
            self.logger.error(f"Error reading data from Kafka '{self.bootstrap_servers}', topic: {self.topic}: {str(e)}")
            raise
    
    def get_source_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the Kafka source.
        
        Returns:
            Dict[str, Any]: Source metadata
        """
        metadata = super().get_source_metadata()
        metadata.update({
            'bootstrap_servers': self.bootstrap_servers,
            'topic': self.topic,
            'group_id': self.group_id,
            'starting_offsets': self.starting_offsets,
            'window_duration': self.window_duration
        })
        return metadata
