"""
Spark session utility for the data ingestion and classification system.
"""

import os
import logging
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "Data Ingestion Pipeline",
    config: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None
) -> SparkSession:
    """
    Create and configure a Spark session.
    
    Args:
        app_name: Name of the Spark application
        config: Additional Spark configuration
        logger: Logger instance
        
    Returns:
        SparkSession: Configured Spark session
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # Start building Spark session
    builder = SparkSession.builder.appName(app_name)
    
    # Apply environment-specific configuration for Cloudera
    builder = builder.config("spark.sql.catalogImplementation", "hive")
    
    # Add Hadoop configuration for Cloudera
    hadoop_conf_dir = os.environ.get("HADOOP_CONF_DIR")
    if hadoop_conf_dir:
        builder = builder.config("spark.hadoop.confDir", hadoop_conf_dir)
    
    # Add YARN configuration if running on YARN
    if os.environ.get("YARN_CONF_DIR"):
        builder = builder.config("spark.yarn.access.hadoopFileSystems", "hdfs://namenode:8020")
    
    # Apply additional configuration
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)
    
    # Enable Hive support if available
    try:
        builder = builder.enableHiveSupport()
    except Exception as e:
        logger.warning(f"Could not enable Hive support: {str(e)}")
    
    # Create session
    logger.info(f"Creating Spark session with app name: {app_name}")
    spark = builder.getOrCreate()
    
    # Configure logging level for Spark
    spark.sparkContext.setLogLevel("WARN")
    
    # Log Spark and Scala versions
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Scala version: {spark.sparkContext._jvm.scala.util.Properties.versionString()}")
    
    return spark
