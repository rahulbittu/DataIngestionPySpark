"""
Connector package for data ingestion from various sources.
This module contains classes for connecting to different data sources
like files, databases, APIs, and Kafka.
"""

from src.connectors.base_connector import BaseConnector
from src.connectors.file_connector import FileConnector
from src.connectors.database_connector import DatabaseConnector
from src.connectors.api_connector import APIConnector
from src.connectors.kafka_connector import KafkaConnector

__all__ = [
    'BaseConnector',
    'FileConnector',
    'DatabaseConnector',
    'APIConnector',
    'KafkaConnector'
]
