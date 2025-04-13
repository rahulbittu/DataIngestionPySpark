"""
File connector for reading data from various file formats.
"""

import os
from typing import Dict, Any, Optional, List
import logging
import glob

from pyspark.sql import DataFrame, SparkSession


from src.connectors.base_connector import BaseConnector


class FileConnector(BaseConnector):
    """Connector for reading data from files"""
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the file connector.
        
        Args:
            spark: The SparkSession
            source_config: Configuration for the file source
            logger: Logger instance
        """
        super().__init__(spark, source_config, logger)
        
        self.path = self.source_config.get('path')
        self.format = self.source_config.get('format', 'csv')
        self.options = self.source_config.get('options', {})
        
    def _validate_source_config(self) -> None:
        """
        Validate file source configuration.
        
        Raises:
            ValueError: If any required configuration is missing
        """
        if not self.path:
            raise ValueError(f"Path must be provided for file source '{self.name}'")
        
        supported_formats = ['csv', 'parquet', 'json', 'orc', 'avro', 'text']
        if self.format not in supported_formats:
            raise ValueError(
                f"Unsupported file format '{self.format}' for source '{self.name}'. "
                f"Supported formats: {', '.join(supported_formats)}"
            )
    
    def connect(self) -> bool:
        """
        Check if the file path exists and is accessible.
        
        Returns:
            bool: True if path exists and is accessible, False otherwise
        """
        try:
            # For HDFS paths, we can use the HDFS API
            if self.path.startswith('hdfs://'):
                # Using SparkContext's Hadoop configuration
                hadoop_conf = self.spark._jsc.hadoopConfiguration()
                hdfs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
                path = self.spark._jvm.org.apache.hadoop.fs.Path(self.path)
                exists = hdfs.exists(path)
                
                if not exists:
                    self.logger.error(f"HDFS path does not exist: {self.path}")
                    return False
                
                self.logger.info(f"Successfully connected to HDFS path: {self.path}")
                return True
            
            # For local filesystem
            if os.path.exists(self.path):
                self.logger.info(f"Successfully connected to file path: {self.path}")
                return True
            else:
                self.logger.error(f"File path does not exist: {self.path}")
                return False
        
        except Exception as e:
            self.logger.error(f"Error connecting to file path '{self.path}': {str(e)}")
            return False
    
    def get_files(self) -> List[str]:
        """
        Get list of files to process.
        
        Returns:
            List[str]: List of file paths
        """
        if os.path.isdir(self.path):
            # If path is a directory, find files with matching format
            pattern = f"{self.path}/*.{self.format}" if self.path.endswith('/') else f"{self.path}/*.{self.format}"
            files = glob.glob(pattern)
            self.logger.info(f"Found {len(files)} {self.format} files in {self.path}")
            return files
        else:
            # If path is a single file
            return [self.path]
    
    def read_data(self) -> DataFrame:
        """
        Read data from file(s) into a Spark DataFrame.
        
        Returns:
            DataFrame: The data as a Spark DataFrame
        
        Raises:
            Exception: If there's an error reading the files
        """
        try:
            reader = self.spark.read
            
            # Apply options
            for key, value in self.options.items():
                reader = reader.option(key, value)
            
            # Read the data using the specified format
            self.logger.info(f"Reading {self.format} data from {self.path}")
            
            df = reader.format(self.format).load(self.path)
            
            row_count = df.count()
            column_count = len(df.columns)
            self.logger.info(f"Successfully read {row_count} rows and {column_count} columns from {self.path}")
            
            return df
        except Exception as e:
            self.logger.error(f"Error reading data from '{self.path}': {str(e)}")
            raise
    
    def get_source_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the file source.
        
        Returns:
            Dict[str, Any]: Source metadata
        """
        metadata = super().get_source_metadata()
        metadata.update({
            'path': self.path,
            'format': self.format,
            'options': self.options
        })
        return metadata
