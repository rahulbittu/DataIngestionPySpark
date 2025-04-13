"""
Base connector that defines the interface for all data source connectors.
"""

from abc import ABC, abstractmethod
import logging
from typing import Dict, Any, Optional

from pyspark.sql import DataFrame, SparkSession


class BaseConnector(ABC):
    """Base class for all data connectors"""

    def __init__(
        self,
        spark: SparkSession,
        source_config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the base connector.
        
        Args:
            spark: The SparkSession
            source_config: Configuration for the data source
            logger: Logger instance
        """
        self.spark = spark
        self.source_config = source_config
        self.name = source_config.get('name', 'unnamed_source')
        self.logger = logger or logging.getLogger(__name__)
        
        # Extract classification rules
        self.classification_rules = source_config.get('classification_rules', {})
        
        # Validate required configuration
        self._validate_config()
    
    def _validate_config(self) -> None:
        """
        Validate that the connector has all required configuration.
        
        Raises:
            ValueError: If any required configuration is missing
        """
        if not self.name:
            raise ValueError("Source name must be provided")
        
        # Connector-specific validation happens in subclasses
        self._validate_source_config()
    
    @abstractmethod
    def _validate_source_config(self) -> None:
        """
        Validate the source-specific configuration.
        
        Raises:
            ValueError: If any required configuration is missing
        """
        pass
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the data source.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        pass
    
    @abstractmethod
    def read_data(self) -> DataFrame:
        """
        Read data from the source into a Spark DataFrame.
        
        Returns:
            DataFrame: The data as a Spark DataFrame
        """
        pass
    
    def get_source_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the source.
        
        Returns:
            Dict[str, Any]: Source metadata
        """
        return {
            'name': self.name,
            'type': self.__class__.__name__,
            'classification_rules': self.classification_rules
        }
    
    def __str__(self) -> str:
        """String representation of the connector"""
        return f"{self.__class__.__name__}(name={self.name})"
