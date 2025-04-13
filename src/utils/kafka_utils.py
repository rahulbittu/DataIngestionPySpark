"""
Kafka utilities for real-time data streaming and event-based processing.
"""

import os
import json
import logging
from typing import Dict, Any, List, Callable, Optional
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

class KafkaStreamingManager:
    """
    Manager for Kafka producers and consumers to handle real-time data streaming.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "data_flow_group",
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the Kafka streaming manager.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
            auto_offset_reset: Auto offset reset configuration ('earliest', 'latest')
            enable_auto_commit: Whether to enable auto commit
            logger: Logger instance
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.logger = logger or logging.getLogger(__name__)
        
        # Keep track of active producers and consumers
        self.producers = {}
        self.consumers = {}
        
        # Message callback registry
        self.message_callbacks = {}
        
    def create_producer(self, name: str) -> Producer:
        """
        Create a Kafka producer.
        
        Args:
            name: Producer name for reference
            
        Returns:
            Producer: Kafka producer instance
        """
        if name in self.producers:
            self.logger.info(f"Producer '{name}' already exists, returning existing instance")
            return self.producers[name]
        
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': f'data-flow-producer-{name}'
        }
        
        try:
            producer = Producer(producer_config)
            self.producers[name] = producer
            self.logger.info(f"Created Kafka producer '{name}'")
            return producer
        except KafkaException as e:
            self.logger.error(f"Failed to create Kafka producer '{name}': {e}")
            raise
    
    def create_consumer(self, name: str, topics: List[str]) -> Consumer:
        """
        Create a Kafka consumer.
        
        Args:
            name: Consumer name for reference
            topics: List of topics to subscribe to
            
        Returns:
            Consumer: Kafka consumer instance
        """
        if name in self.consumers:
            self.logger.info(f"Consumer '{name}' already exists, returning existing instance")
            return self.consumers[name]
        
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f'{self.group_id}-{name}',
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit
        }
        
        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe(topics)
            self.consumers[name] = consumer
            self.logger.info(f"Created Kafka consumer '{name}' subscribed to topics: {topics}")
            return consumer
        except KafkaException as e:
            self.logger.error(f"Failed to create Kafka consumer '{name}': {e}")
            raise
    
    def produce_message(
        self,
        producer_name: str,
        topic: str,
        value: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[List[Dict[str, Any]]] = None,
        callback: Optional[Callable] = None
    ) -> None:
        """
        Produce a message to a Kafka topic.
        
        Args:
            producer_name: Producer name
            topic: Topic to produce to
            value: Message value (will be serialized to JSON)
            key: Optional message key
            headers: Optional message headers
            callback: Optional delivery callback
        """
        if producer_name not in self.producers:
            self.logger.warning(f"Producer '{producer_name}' not found, creating it")
            self.create_producer(producer_name)
        
        producer = self.producers[producer_name]
        
        try:
            # Serialize value to JSON
            value_bytes = json.dumps(value).encode('utf-8')
            
            # Serialize key to bytes if provided
            key_bytes = key.encode('utf-8') if key else None
            
            # Produce message
            producer.produce(
                topic=topic,
                value=value_bytes,
                key=key_bytes,
                headers=headers,
                callback=callback
            )
            
            # Flush to ensure message is sent
            producer.poll(0)
            
        except Exception as e:
            self.logger.error(f"Error producing message to topic '{topic}': {e}")
            raise
    
    def register_message_handler(
        self,
        consumer_name: str,
        callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """
        Register a callback function to handle messages from a consumer.
        
        Args:
            consumer_name: Consumer name
            callback: Callback function that takes a message value dict
        """
        self.message_callbacks[consumer_name] = callback
        self.logger.info(f"Registered message handler for consumer '{consumer_name}'")
    
    def start_consuming(self, consumer_name: str, timeout: float = 1.0) -> None:
        """
        Start consuming messages with the specified consumer.
        
        Args:
            consumer_name: Consumer name
            timeout: Poll timeout in seconds
        """
        if consumer_name not in self.consumers:
            raise ValueError(f"Consumer '{consumer_name}' not found")
        
        consumer = self.consumers[consumer_name]
        callback = self.message_callbacks.get(consumer_name)
        
        if not callback:
            self.logger.warning(f"No message handler registered for consumer '{consumer_name}'")
            return
        
        try:
            self.logger.info(f"Starting to consume messages for consumer '{consumer_name}'")
            
            while True:
                msg = consumer.poll(timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.debug(f"Reached end of partition for topic {msg.topic()}")
                    else:
                        self.logger.error(f"Error consuming message: {msg.error()}")
                else:
                    try:
                        # Deserialize message value
                        value_str = msg.value().decode('utf-8')
                        value = json.loads(value_str)
                        
                        # Call the registered callback
                        callback(value)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing message: {e}")
        
        except KeyboardInterrupt:
            self.logger.info(f"Stopped consuming messages for consumer '{consumer_name}'")
        finally:
            consumer.close()
    
    def start_consuming_batch(
        self,
        consumer_name: str,
        batch_size: int = 100,
        timeout: float = 1.0,
        batch_callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> None:
        """
        Start consuming messages in batches with the specified consumer.
        
        Args:
            consumer_name: Consumer name
            batch_size: Number of messages to collect before processing
            timeout: Poll timeout in seconds
            batch_callback: Optional callback for batch processing
        """
        if consumer_name not in self.consumers:
            raise ValueError(f"Consumer '{consumer_name}' not found")
        
        consumer = self.consumers[consumer_name]
        callback = batch_callback or self.message_callbacks.get(consumer_name)
        
        if not callback:
            self.logger.warning(f"No message handler registered for consumer '{consumer_name}'")
            return
        
        try:
            self.logger.info(f"Starting to consume messages in batches for consumer '{consumer_name}'")
            
            while True:
                batch = []
                
                # Collect batch of messages
                for _ in range(batch_size):
                    msg = consumer.poll(timeout)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            self.logger.debug(f"Reached end of partition for topic {msg.topic()}")
                        else:
                            self.logger.error(f"Error consuming message: {msg.error()}")
                    else:
                        try:
                            # Deserialize message value
                            value_str = msg.value().decode('utf-8')
                            value = json.loads(value_str)
                            batch.append(value)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing message: {e}")
                
                # Process batch if not empty
                if batch:
                    try:
                        callback(batch)
                    except Exception as e:
                        self.logger.error(f"Error processing batch: {e}")
        
        except KeyboardInterrupt:
            self.logger.info(f"Stopped consuming messages for consumer '{consumer_name}'")
        finally:
            consumer.close()
    
    def close(self) -> None:
        """
        Close all producers and consumers.
        """
        for name, producer in self.producers.items():
            try:
                producer.flush()
                self.logger.info(f"Closed producer '{name}'")
            except Exception as e:
                self.logger.error(f"Error closing producer '{name}': {e}")
        
        for name, consumer in self.consumers.items():
            try:
                consumer.close()
                self.logger.info(f"Closed consumer '{name}'")
            except Exception as e:
                self.logger.error(f"Error closing consumer '{name}': {e}")