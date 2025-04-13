"""
Hive connector for writing data to Hive tables.
"""

import os
import re
import logging
from typing import Dict, Any, List, Optional, Tuple, Union

from pyspark.sql import SparkSession, DataFrame
from pyhive import hive
import pyspark.sql.functions as F

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
        
        # Extract Hive configuration
        self.database = source_config.get('database', 'default')
        self.host = source_config.get('host', 'localhost')
        self.port = source_config.get('port', 10000)
        self.auth_mechanism = source_config.get('auth_mechanism', 'NONE')
        self.username = source_config.get('username', None)
        self.password = source_config.get('password', None)
        self.save_mode = source_config.get('save_mode', 'append')
        
        # Table configuration
        self.table_prefix = source_config.get('table_prefix', '')
        self.partition_columns = source_config.get('partition_columns', ['year', 'month', 'day'])
        
        # Connection status
        self.connection = None
        
        # Validate config
        self._validate_source_config()
        
    def _resolve_env_var(self, value: str) -> str:
        """
        Resolve environment variables in string.
        
        Args:
            value: String that may contain environment variable references like ${VAR_NAME}
            
        Returns:
            str: String with environment variables replaced with their values
        """
        if value is None:
            return None
            
        # Match ${VAR_NAME} pattern
        pattern = r'\${([A-Za-z0-9_]+)}'
        
        def replace_env_var(match):
            env_var = match.group(1)
            return os.environ.get(env_var, match.group(0))
            
        return re.sub(pattern, replace_env_var, value)
        
    def _validate_source_config(self) -> None:
        """
        Validate Hive target configuration.
        
        Raises:
            ValueError: If any required configuration is missing
        """
        missing_fields = []
        
        if not self.database:
            missing_fields.append('database')
            
        if not self.host:
            missing_fields.append('host')
            
        if missing_fields:
            raise ValueError(f"Missing required Hive configuration: {', '.join(missing_fields)}")
            
        # If auth mechanism requires username/password, validate those
        if self.auth_mechanism.upper() in ['LDAP', 'KERBEROS', 'CUSTOM']:
            if not self.username:
                self.logger.warning("Auth mechanism requires username but none provided")
                
            if not self.password and self.auth_mechanism.upper() in ['LDAP', 'CUSTOM']:
                self.logger.warning("Auth mechanism may require password but none provided")
                
    def connect(self) -> bool:
        """
        Test the Hive connection.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            # Resolve environment variables in credentials
            host = self._resolve_env_var(self.host)
            username = self._resolve_env_var(self.username)
            password = self._resolve_env_var(self.password)
            
            self.logger.info(f"Connecting to Hive at {host}:{self.port}, database: {self.database}")
            
            # Connect to Hive
            self.connection = hive.Connection(
                host=host,
                port=self.port,
                username=username,
                password=password,
                database=self.database,
                auth=self.auth_mechanism
            )
            
            # Test query
            cursor = self.connection.cursor()
            cursor.execute('SHOW TABLES')
            tables = cursor.fetchall()
            
            self.logger.info(f"Successfully connected to Hive. Tables in {self.database}: {len(tables)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Hive: {str(e)}")
            self.connection = None
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
            # Generate table name from classification and source
            table_name = f"{self.table_prefix}{classification}_{source_name}"
            table_name = table_name.lower().replace('-', '_')
            
            self.logger.info(f"Writing {df.count()} records to Hive table: {self.database}.{table_name}")
            
            # Create partitioning columns if timestamp is provided
            if timestamp:
                # Extract year, month, day from timestamp (expects YYYY-MM-DD format)
                parts = timestamp.split('-')
                if len(parts) >= 3:
                    year, month, day = parts[0], parts[1], parts[2]
                    
                    # Add partition columns
                    output_df = df
                    if 'year' in self.partition_columns:
                        output_df = output_df.withColumn('year', F.lit(year))
                    if 'month' in self.partition_columns:
                        output_df = output_df.withColumn('month', F.lit(month))
                    if 'day' in self.partition_columns:
                        output_df = output_df.withColumn('day', F.lit(day))
                else:
                    # If timestamp doesn't match expected format, just use original df
                    output_df = df
            else:
                output_df = df
            
            # Write to Hive table
            output_df.write.format("hive") \
                .mode(self.save_mode) \
                .partitionBy(*[col for col in self.partition_columns if col in output_df.columns]) \
                .saveAsTable(f"{self.database}.{table_name}")
            
            self.logger.info(f"Successfully wrote data to Hive table: {self.database}.{table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing data to Hive: {str(e)}")
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
            # This is a placeholder since this connector is primarily for writing
            # It would normally retrieve data from a specified table
            if not self.source_config.get('table_name'):
                raise ValueError("No table_name specified for reading from Hive")
                
            table_name = self.source_config.get('table_name')
            
            # Read the table
            df = self.spark.read.table(f"{self.database}.{table_name}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading data from Hive: {str(e)}")
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
            if not self.connection:
                if not self.connect():
                    return False
                    
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            tables = cursor.fetchall()
            
            return len(tables) > 0
            
        except Exception as e:
            self.logger.error(f"Error checking if table {table_name} exists: {str(e)}")
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
            if not self.connection:
                if not self.connect():
                    return []
                    
            cursor = self.connection.cursor()
            cursor.execute(f"DESCRIBE {self.database}.{table_name}")
            columns = cursor.fetchall()
            
            schema = []
            for col in columns:
                # Format depends on hive version, but typically col[0] is name, col[1] is type
                if len(col) >= 2:
                    schema.append({
                        'name': col[0],
                        'type': col[1]
                    })
                    
            return schema
            
        except Exception as e:
            self.logger.error(f"Error getting schema for table {table_name}: {str(e)}")
            return []
            
    def get_source_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the Hive source/target.
        
        Returns:
            Dict[str, Any]: Source metadata
        """
        return {
            'name': self.source_config.get('name', 'hive'),
            'type': 'hive',
            'database': self.database,
            'host': self.host,
            'tables': self._get_tables()
        }
        
    def _get_tables(self) -> List[str]:
        """
        Get list of tables in the configured database.
        
        Returns:
            List[str]: List of table names
        """
        try:
            if not self.connection:
                if not self.connect():
                    return []
                    
            cursor = self.connection.cursor()
            cursor.execute('SHOW TABLES')
            tables = cursor.fetchall()
            
            return [table[0] for table in tables]
            
        except Exception as e:
            self.logger.error(f"Error getting tables: {str(e)}")
            return []
            
    def write_streaming(
        self,
        streaming_df: DataFrame,
        classification: str,
        source_name: str,
        checkpoint_dir: str,
        query_name: Optional[str] = None,
        trigger_interval: Optional[str] = None
    ) -> Any:
        """
        Write streaming data to a Hive table.
        
        Args:
            streaming_df: Streaming DataFrame to write
            classification: Data classification (bronze, silver, gold, rejected)
            source_name: Name of the source
            checkpoint_dir: Checkpoint directory
            query_name: Optional name for the streaming query
            trigger_interval: Optional trigger interval
            
        Returns:
            Any: The streaming query
        """
        try:
            # Generate table name from classification and source
            table_name = f"{self.table_prefix}{classification}_{source_name}"
            table_name = table_name.lower().replace('-', '_')
            
            self.logger.info(f"Writing streaming data to Hive table: {self.database}.{table_name}")
            
            # Create query name if not provided
            actual_query_name = query_name or f"hive-stream-{classification}-{source_name}"
            
            # Prepare streaming writer with ForeachBatch to handle Hive writes
            def foreach_batch_function(batch_df, batch_id):
                # Add batch metadata
                output_df = batch_df.withColumn('batch_id', F.lit(batch_id))
                
                # Add partitioning columns - use current date/time
                current_date = F.current_date()
                year = F.year(current_date)
                month = F.month(current_date)
                day = F.dayofmonth(current_date)
                
                if 'year' in self.partition_columns:
                    output_df = output_df.withColumn('year', year)
                if 'month' in self.partition_columns:
                    output_df = output_df.withColumn('month', month)
                if 'day' in self.partition_columns:
                    output_df = output_df.withColumn('day', day)
                
                # Write to Hive using batch mode
                if batch_df.count() > 0:
                    output_df.write.format("hive") \
                        .mode(self.save_mode) \
                        .partitionBy(*[col for col in self.partition_columns if col in output_df.columns]) \
                        .saveAsTable(f"{self.database}.{table_name}")
                    
                    self.logger.info(f"Successfully wrote batch {batch_id} to Hive table: {self.database}.{table_name}")
            
            # Create the streaming writer
            writer = streaming_df.writeStream \
                .foreachBatch(foreach_batch_function) \
                .option("checkpointLocation", checkpoint_dir) \
                .queryName(actual_query_name)
            
            # Add trigger if specified
            if trigger_interval:
                writer = writer.trigger(processingTime=trigger_interval)
            
            # Start the streaming query
            query = writer.start()
            
            self.logger.info(f"Started streaming query to write to Hive table: {self.database}.{table_name}")
            return query
            
        except Exception as e:
            self.logger.error(f"Error setting up Hive streaming: {str(e)}")
            raise