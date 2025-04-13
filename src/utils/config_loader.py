"""
Configuration loader for the data ingestion and classification system.
"""

import os
import yaml
from typing import Dict, Any, Optional, List


class ConfigLoader:
    """
    Utility class for loading and accessing configuration files.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize the config loader.
        
        Args:
            config_path: Path to the YAML configuration file
        
        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            ValueError: If the configuration file is invalid
        """
        self.config_path = config_path
        
        # Check if configuration file exists
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        # Load configuration
        try:
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML configuration: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error loading configuration: {str(e)}")
    
    def get_file_sources(self) -> List[Dict[str, Any]]:
        """
        Get configuration for file sources.
        
        Returns:
            List[Dict[str, Any]]: List of file source configurations
        """
        return self.config.get('file_sources', [])
    
    def get_database_sources(self) -> List[Dict[str, Any]]:
        """
        Get configuration for database sources.
        
        Returns:
            List[Dict[str, Any]]: List of database source configurations
        """
        return self.config.get('database_sources', [])
    
    def get_api_sources(self) -> List[Dict[str, Any]]:
        """
        Get configuration for API sources.
        
        Returns:
            List[Dict[str, Any]]: List of API source configurations
        """
        return self.config.get('api_sources', [])
    
    def get_kafka_sources(self) -> List[Dict[str, Any]]:
        """
        Get configuration for Kafka sources.
        
        Returns:
            List[Dict[str, Any]]: List of Kafka source configurations
        """
        return self.config.get('kafka_sources', [])
    
    def get_all_sources(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get configuration for all data sources.
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary of all source configurations
        """
        return {
            'file_sources': self.get_file_sources(),
            'database_sources': self.get_database_sources(),
            'api_sources': self.get_api_sources(),
            'kafka_sources': self.get_kafka_sources()
        }
    
    def get_classification_thresholds(self) -> Dict[str, Dict[str, float]]:
        """
        Get classification thresholds for bronze, silver, and gold data.
        
        Returns:
            Dict[str, Dict[str, float]]: Classification thresholds
        """
        return {
            'bronze': self.config.get('default_classification', {}).get('bronze', {}),
            'silver': self.config.get('default_classification', {}).get('silver', {}),
            'gold': self.config.get('default_classification', {}).get('gold', {})
        }
    
    def get_target_paths(self) -> Dict[str, str]:
        """
        Get target paths for different classification levels.
        
        Returns:
            Dict[str, str]: Dictionary mapping classification levels to output paths
        """
        return self.config.get('target_paths', {})
    
    def resolve_env_vars_in_config(self, config_item: Any) -> Any:
        """
        Recursively resolve environment variables in configuration.
        
        Args:
            config_item: Configuration item (can be dict, list, or scalar)
            
        Returns:
            Any: Configuration with environment variables resolved
        """
        if isinstance(config_item, dict):
            return {k: self.resolve_env_vars_in_config(v) for k, v in config_item.items()}
        elif isinstance(config_item, list):
            return [self.resolve_env_vars_in_config(v) for v in config_item]
        elif isinstance(config_item, str) and config_item.startswith('${') and config_item.endswith('}'):
            env_var = config_item[2:-1]
            return os.environ.get(env_var, config_item)
        else:
            return config_item
    
    def get_source_by_name(self, source_name: str) -> Optional[Dict[str, Any]]:
        """
        Get source configuration by name.
        
        Args:
            source_name: Name of the source
            
        Returns:
            Optional[Dict[str, Any]]: Source configuration or None if not found
        """
        all_sources = []
        all_sources.extend(self.get_file_sources())
        all_sources.extend(self.get_database_sources())
        all_sources.extend(self.get_api_sources())
        all_sources.extend(self.get_kafka_sources())
        
        for source in all_sources:
            if source.get('name') == source_name:
                return source
        
        return None
        
    def get_elasticsearch_config(self) -> Optional[Dict[str, Any]]:
        """
        Get Elasticsearch configuration.
        
        Returns:
            Optional[Dict[str, Any]]: Elasticsearch configuration or None if not found
        """
        es_config = self.config.get('elasticsearch', None)
        
        if es_config:
            # Resolve any environment variables in the config
            es_config = self.resolve_env_vars_in_config(es_config)
            
        return es_config
