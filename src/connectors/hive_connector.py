"""
Hive connector for writing data to Hive tables.
"""

import os
import logging
from typing import Dict, Any, Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyhive import hive

from src.connectors.base_connector import BaseConnector


class HiveConnector(BaseConnector):
    """Connector for writing data to Hive tables and managing Hive metadata"""

    def __init__(
        self,
        spark: SparkSession,
        source_config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the Hive connector.
        
        Args:
            spark: The SparkSession
            source_config: Configuration for the Hive target
            logger: Logger instance
        """
        super().__init__(spark, source_config, logger)
        
        # Extract Hive-specific configurations
        self.database = self.source_config.get('database', 'default')
        self.table_prefix = self.source_config.get('table_prefix', '')
        self.host = self._resolve_env_var(self.source_config.get('host', 'localhost'))
        self.port = int(self._resolve_env_var(str(self.source_config.get('port', 10000))))
        self.username = self._resolve_env_var(self.source_config.get('username', ''))
        self.password = self._resolve_env_var(self.source_config.get('password', ''))
        self.auth_mechanism = self.source_config.get('auth_mechanism', 'NONE')
        self.partition_columns = self.source_config.get('partition_columns', [])
        self.mode = self.source_config.get('save_mode', 'append')
        
        # Additional Hive settings
        self.hive_settings = self.source_config.get('hive_settings', {})
    
    def _resolve_env_var(self, value: str) -> str:
        """
        Resolve environment variables in string.
        
        Args:
            value: String that may contain environment variable references like ${VAR_NAME}
            
        Returns:
            str: String with environment variables replaced with their values
        """
        if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
            env_var = value[2:-1]
            return os.environ.get(env_var, value)
        return value
    
    def _validate_source_config(self) -> None:
        """
        Validate Hive target configuration.
        
        Raises:
            ValueError: If any required configuration is missing
        """
        required_fields = ['name', 'type']
        missing_fields = [field for field in required_fields if field not in self.source_config]
        
        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")
        
        if self.source_config.get('type') != 'hive':
            raise ValueError("Source type must be 'hive'")
    
    def connect(self) -> bool:
        """
        Test the Hive connection.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            self.logger.info(f"Testing Hive connection to {self.host}:{self.port}/{self.database}")
            
            # Try to execute a simple query using PyHive
            connection = hive.Connection(
                host=self.host,
                port=self.port,
                username=self.username if self.username else None,
                password=self.password if self.password else None,
                database=self.database,
                auth=self.auth_mechanism
            )
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            self.logger.info(f"Connected to Hive. {len(tables)} tables found in database '{self.database}'")
            cursor.close()
            connection.close()
            
            # Also check if Hive catalog is accessible via Spark
            tables_df = self.spark.sql(f"SHOW TABLES IN {self.database}")
            self.logger.info(f"Hive catalog accessible from Spark: {tables_df.count()} tables found")
            
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to Hive: {str(e)}")
            return False
    
    def write_data(self, df: DataFrame, classification: str, source_name: str, timestamp: str = None) -> bool:
        """
        Write data to a Hive table.
        
        Args:
            df: DataFrame to write
            classification: Data classification (bronze, silver, gold, rejected)
            source_name: Name of the source
            timestamp: Optional timestamp string for partitioning
            
        Returns:
            bool: True if writing is successful, False otherwise
        """
        try:
            # Construct table name with optional prefix
            table_name = f"{self.table_prefix}{classification}_{source_name}"
            if not table_name.isidentifier():
                table_name = table_name.replace('-', '_').replace('.', '_')
            
            full_table_name = f"{self.database}.{table_name}"
            self.logger.info(f"Writing {df.count()} rows to Hive table: {full_table_name}")
            
            # Apply Hive settings
            for setting, value in self.hive_settings.items():
                self.spark.sql(f"SET {setting}={value}")
            
            # Create the database if it doesn't exist
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            
            # Write data to Hive table
            writer = df.write.mode(self.mode)
            
            # Add partitioning if specified
            if self.partition_columns:
                writer = writer.partitionBy(*self.partition_columns)
                
            # Add timestamp partition if provided
            if timestamp and 'ingestion_date' not in self.partition_columns:
                # Add ingestion_date column if it doesn't exist
                if 'ingestion_date' not in df.columns:
                    from pyspark.sql.functions import lit
                    df = df.withColumn('ingestion_date', lit(timestamp))
                writer = writer.partitionBy(*self.partition_columns, 'ingestion_date')
            
            # Save as Hive table
            writer.saveAsTable(full_table_name)
            
            self.logger.info(f"Successfully wrote data to Hive table: {full_table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing to Hive table: {str(e)}")
            return False
    
    def read_data(self) -> DataFrame:
        """
        Read data from a Hive table into a Spark DataFrame.
        
        Returns:
            DataFrame: The data as a Spark DataFrame
        
        Raises:
            Exception: If there's an error reading from Hive
        """
        try:
            table_name = self.source_config.get('table')
            if not table_name:
                raise ValueError("Table name must be specified for reading from Hive")
                
            full_table_name = f"{self.database}.{table_name}"
            self.logger.info(f"Reading from Hive table: {full_table_name}")
            
            # Apply any Hive settings
            for setting, value in self.hive_settings.items():
                self.spark.sql(f"SET {setting}={value}")
            
            # Read the table
            return self.spark.table(full_table_name)
            
        except Exception as e:
            self.logger.error(f"Error reading from Hive table: {str(e)}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in Hive.
        
        Args:
            table_name: Name of the table (without database)
            
        Returns:
            bool: True if the table exists, False otherwise
        """
        try:
            full_table_name = f"{self.database}.{table_name}"
            tables = self.spark.sql(f"SHOW TABLES IN {self.database}").collect()
            return any(row.tableName == table_name for row in tables)
        except Exception as e:
            self.logger.error(f"Error checking if table exists: {str(e)}")
            return False

    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get the schema of a Hive table.
        
        Args:
            table_name: Name of the table (without database)
            
        Returns:
            List[Dict[str, str]]: List of column definitions
        """
        try:
            full_table_name = f"{self.database}.{table_name}"
            columns = self.spark.sql(f"DESCRIBE {full_table_name}").collect()
            schema = []
            
            for col in columns:
                if col.col_name and not col.col_name.startswith('#'):
                    schema.append({
                        'name': col.col_name,
                        'type': col.data_type,
                        'comment': col.comment if hasattr(col, 'comment') else ''
                    })
            
            return schema
        except Exception as e:
            self.logger.error(f"Error getting table schema: {str(e)}")
            return []
    
    def get_source_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the Hive source/target.
        
        Returns:
            Dict[str, Any]: Source metadata
        """
        return {
            'name': self.source_config.get('name'),
            'type': 'hive',
            'database': self.database,
            'host': self.host,
            'port': self.port,
            'tables': self._get_tables(),
            'auth_type': self.auth_mechanism
        }
    
    def _get_tables(self) -> List[str]:
        """
        Get list of tables in the configured database.
        
        Returns:
            List[str]: List of table names
        """
        try:
            tables = self.spark.sql(f"SHOW TABLES IN {self.database}").collect()
            return [row.tableName for row in tables]
        except Exception as e:
            self.logger.error(f"Error getting table list: {str(e)}")
            return []