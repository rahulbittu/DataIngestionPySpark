"""
Database connector for reading data from relational databases.
"""

import os
import logging
from typing import Dict, Any, Optional

from pyspark.sql import DataFrame, SparkSession

from src.connectors.base_connector import BaseConnector


class DatabaseConnector(BaseConnector):
    """Connector for reading data from relational databases"""
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the database connector.
        
        Args:
            spark: The SparkSession
            source_config: Configuration for the database source
            logger: Logger instance
        """
        super().__init__(spark, source_config, logger)
        
        # Extract database configuration
        self.connection_string = self.source_config.get('connection_string')
        self.driver = self.source_config.get('driver')
        self.table = self.source_config.get('table')
        self.query = self.source_config.get('query')
        
        # Get credentials from environment variables if needed
        self.user = self._resolve_env_var(self.source_config.get('user', ''))
        self.password = self._resolve_env_var(self.source_config.get('password', ''))
        
        # Additional JDBC options
        self.jdbc_options = self.source_config.get('jdbc_options', {})
    
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
        Validate database source configuration.
        
        Raises:
            ValueError: If any required configuration is missing
        """
        if not self.connection_string:
            raise ValueError(f"Connection string must be provided for database source '{self.name}'")
        
        if not self.driver:
            raise ValueError(f"JDBC driver must be provided for database source '{self.name}'")
        
        if not self.table and not self.query:
            raise ValueError(f"Either table or query must be provided for database source '{self.name}'")
    
    def connect(self) -> bool:
        """
        Test the database connection.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            # Create a small test query to verify connection
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.connection_string) \
                .option("driver", self.driver) \
                .option("query", "SELECT 1") \
                .option("user", self.user) \
                .option("password", self.password) \
                .load()
            
            # Force execution of the query to test connection
            test_df.count()
            
            self.logger.info(f"Successfully connected to database: {self.connection_string}")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to database '{self.connection_string}': {str(e)}")
            return False
    
    def read_data(self) -> DataFrame:
        """
        Read data from the database into a Spark DataFrame.
        
        Returns:
            DataFrame: The data as a Spark DataFrame
        
        Raises:
            Exception: If there's an error reading from database
        """
        try:
            reader = self.spark.read.format("jdbc") \
                .option("url", self.connection_string) \
                .option("driver", self.driver) \
                .option("user", self.user) \
                .option("password", self.password)
            
            # Add any additional JDBC options
            for key, value in self.jdbc_options.items():
                reader = reader.option(key, value)
            
            # Use either table or query
            if self.query:
                self.logger.info(f"Executing query on {self.connection_string}")
                reader = reader.option("query", self.query)
            else:
                self.logger.info(f"Reading table {self.table} from {self.connection_string}")
                reader = reader.option("dbtable", self.table)
            
            df = reader.load()
            
            row_count = df.count()
            column_count = len(df.columns)
            self.logger.info(f"Successfully read {row_count} rows and {column_count} columns from database")
            
            return df
        except Exception as e:
            self.logger.error(f"Error reading data from database '{self.connection_string}': {str(e)}")
            raise
    
    def get_source_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the database source.
        
        Returns:
            Dict[str, Any]: Source metadata
        """
        metadata = super().get_source_metadata()
        metadata.update({
            'connection_string': self.connection_string,
            'driver': self.driver,
            'table': self.table,
            'has_query': self.query is not None
        })
        return metadata
