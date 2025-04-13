"""
Logging utilities for the data ingestion and classification system.
"""

import os
import logging
import logging.config
import yaml
from typing import Optional, Dict, Any


def setup_logging(
    config_path: Optional[str] = None,
    default_level: int = logging.INFO,
    env_key: str = 'LOG_CONFIG'
) -> logging.Logger:
    """
    Set up logging configuration from a YAML file.
    
    Args:
        config_path: Path to the logging configuration file
        default_level: Default logging level
        env_key: Environment variable that can specify logging config file path
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Check if log path specified in environment variable
    config_env_path = os.environ.get(env_key, None)
    if config_env_path:
        config_path = config_env_path
    
    # Set up configuration
    if config_path and os.path.exists(config_path):
        with open(config_path, 'rt') as f:
            try:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)
                logger = logging.getLogger()
                logger.info(f"Logging configured using file: {config_path}")
                return logger
            except Exception as e:
                print(f"Error loading logging configuration from {config_path}: {e}")
                # Fallback to basic config
                logging.basicConfig(level=default_level)
                logger = logging.getLogger()
                logger.warning(f"Using basic logging config with level {default_level}")
                return logger
    else:
        # Fallback to basic config
        logging.basicConfig(level=default_level)
        logger = logging.getLogger()
        logger.warning(f"Using basic logging config with level {default_level}")
        return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Logger name
        
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)


def log_dataframe_info(logger: logging.Logger, df, source_name: str) -> None:
    """
    Log information about a Spark DataFrame.
    
    Args:
        logger: Logger instance
        df: Spark DataFrame
        source_name: Name of the data source
    """
    try:
        row_count = df.count()
        column_count = len(df.columns)
        logger.info(f"DataFrame from {source_name}: {row_count} rows, {column_count} columns")
        
        # Log schema
        logger.debug(f"DataFrame schema from {source_name}: {df.schema.simpleString()}")
        
        # Log sample data (first 5 rows)
        if row_count > 0:
            sample_rows = df.limit(5).collect()
            logger.debug(f"Sample data from {source_name} (first 5 rows): {sample_rows}")
    except Exception as e:
        logger.error(f"Error logging DataFrame info for {source_name}: {str(e)}")


def log_metrics(logger: logging.Logger, metrics: Dict[str, Any], source_name: str) -> None:
    """
    Log data quality metrics.
    
    Args:
        logger: Logger instance
        metrics: Dictionary of metrics
        source_name: Name of the data source
    """
    logger.info(f"Data quality metrics for {source_name}:")
    for metric_name, metric_value in metrics.items():
        if isinstance(metric_value, float):
            logger.info(f"  {metric_name}: {metric_value:.4f}")
        else:
            logger.info(f"  {metric_name}: {metric_value}")
