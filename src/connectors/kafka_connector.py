"""
Kafka connector for reading streaming data from Kafka topics.
Supports both batch and streaming modes for real-time data processing.
"""

import os
import json
import logging
import threading
from typing import Dict, Any, Optional, List, Callable, Union, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json, window, current_timestamp
from pyspark.sql.types import StructType, StringType
from pyspark.sql.streaming import StreamingQuery

from src.connectors.base_connector import BaseConnector
from src.utils.kafka_utils import KafkaStreamingManager


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
    
    def _resolve_env_var(self, value: Optional[str]) -> Optional[str]:
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
        
    def start_streaming(
        self,
        process_func: Callable[[DataFrame], DataFrame],
        output_mode: str = "append",
        checkpoint_location: str = "./checkpoints",
        query_name: Optional[str] = None,
        trigger_interval: Optional[str] = None,
        output_sink: Optional[str] = None,
        sink_options: Optional[Dict[str, str]] = None
    ) -> StreamingQuery:
        """
        Start a streaming query with the given processing function.
        
        Args:
            process_func: Function to process the streaming DataFrame
            output_mode: Output mode for the streaming query (append, complete, update)
            checkpoint_location: Location to store checkpoints
            query_name: Name for the streaming query
            trigger_interval: Processing interval (e.g., "10 seconds")
            output_sink: Optional output sink format (e.g., "console", "memory", "kafka")
            sink_options: Optional configuration for the output sink
            
        Returns:
            StreamingQuery: The streaming query
            
        Raises:
            Exception: If there's an error starting the streaming query
        """
        try:
            # Read data from Kafka
            stream_df = self.read_data()
            
            # Apply the processing function
            processed_df = process_func(stream_df)
            
            # Add processing timestamp if not already present
            if "_processing_time" not in processed_df.columns:
                processed_df = processed_df.withColumn("_processing_time", current_timestamp())
            
            # Create the streaming writer
            query_name = query_name or f"streaming-query-{self.name}"
            writer = processed_df.writeStream \
                .outputMode(output_mode) \
                .option("checkpointLocation", f"{checkpoint_location}/{query_name}") \
                .queryName(query_name)
            
            # Add trigger if specified
            if trigger_interval:
                writer = writer.trigger(processingTime=trigger_interval)
            
            # Configure the output sink
            if output_sink == "console":
                query = writer.format("console").start()
            elif output_sink == "memory":
                query = writer.format("memory").start()
            elif output_sink == "kafka":
                if not sink_options or "kafka.bootstrap.servers" not in sink_options:
                    sink_options = sink_options or {}
                    sink_options["kafka.bootstrap.servers"] = self.bootstrap_servers
                
                for key, value in sink_options.items():
                    writer = writer.option(key, value)
                
                query = writer.format("kafka").start()
            else:
                # Default to foreachBatch for custom handling
                query = writer.foreachBatch(self._foreach_batch_handler).start()
            
            self.logger.info(f"Started streaming query '{query_name}' with output mode '{output_mode}'")
            return query
            
        except Exception as e:
            self.logger.error(f"Error starting streaming query: {str(e)}")
            raise
    
    def _foreach_batch_handler(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Default handler for foreachBatch processing.
        
        Args:
            batch_df: The batch DataFrame
            batch_id: The batch ID
        """
        if batch_df.isEmpty():
            return
        
        self.logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
        
        # Implement default handling logic here
        # For now, just log batch statistics
        try:
            count = batch_df.count()
            self.logger.info(f"Batch {batch_id} contained {count} records")
        except Exception as e:
            self.logger.error(f"Error processing batch {batch_id}: {str(e)}")
    
    def create_streaming_manager(self) -> KafkaStreamingManager:
        """
        Create a Kafka streaming manager for direct Kafka operations.
        
        Returns:
            KafkaStreamingManager: Kafka streaming manager instance
        """
        return KafkaStreamingManager(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.starting_offsets,
            logger=self.logger
        )
    
    def start_event_stream(
        self,
        consumer_name: str,
        process_func: Callable[[Dict[str, Any]], None],
        batch_mode: bool = False,
        batch_size: int = 100,
        run_in_thread: bool = True
    ) -> Optional[threading.Thread]:
        """
        Start consuming events from Kafka topic with direct event processing.
        
        Args:
            consumer_name: Name for the consumer
            process_func: Function to process each event
            batch_mode: Whether to process events in batches
            batch_size: Size of batches when batch_mode is True
            run_in_thread: Whether to run consumer in a separate thread
            
        Returns:
            Optional[threading.Thread]: Thread if run_in_thread is True, None otherwise
        """
        try:
            # Create streaming manager
            manager = self.create_streaming_manager()
            
            # Create consumer
            manager.create_consumer(consumer_name, [self.topic])
            
            # Register message handler
            manager.register_message_handler(consumer_name, process_func)
            
            # Define consumer function
            def consume_events():
                try:
                    if batch_mode:
                        manager.start_consuming_batch(consumer_name, batch_size=batch_size)
                    else:
                        manager.start_consuming(consumer_name)
                except Exception as e:
                    self.logger.error(f"Error in Kafka consumer thread: {str(e)}")
                finally:
                    manager.close()
            
            # Start consumer
            if run_in_thread:
                thread = threading.Thread(
                    target=consume_events,
                    name=f"kafka-consumer-{consumer_name}",
                    daemon=True
                )
                thread.start()
                self.logger.info(f"Started Kafka consumer '{consumer_name}' in thread")
                return thread
            else:
                self.logger.info(f"Starting Kafka consumer '{consumer_name}'")
                consume_events()
                return None
                
        except Exception as e:
            self.logger.error(f"Error starting Kafka event stream: {str(e)}")
            raise
    
    def produce_event(
        self,
        value: Dict[str, Any],
        key: Optional[str] = None,
        producer_name: str = "default-producer",
        topic: Optional[str] = None
    ) -> None:
        """
        Produce a single event to a Kafka topic.
        
        Args:
            value: Event data
            key: Optional message key
            producer_name: Name for the producer
            topic: Topic to produce to (defaults to the source topic)
        """
        try:
            # Create streaming manager
            manager = self.create_streaming_manager()
            
            # Create producer if needed
            if producer_name not in manager.producers:
                manager.create_producer(producer_name)
            
            # Use the source topic by default
            target_topic = topic or self.topic
            
            # Add timestamp if not present
            if "_timestamp" not in value:
                import time
                value["_timestamp"] = int(time.time() * 1000)
            
            # Produce message
            manager.produce_message(
                producer_name=producer_name,
                topic=target_topic,
                value=value,
                key=key
            )
            
            self.logger.debug(f"Produced event to topic {target_topic}")
            
        except Exception as e:
            self.logger.error(f"Error producing event to Kafka: {str(e)}")
            raise
