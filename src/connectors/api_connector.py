"""
API connector for reading data from REST APIs.
"""

import os
import json
import logging
from typing import Dict, Any, Optional

import requests
from pyspark.sql import DataFrame, SparkSession

from src.connectors.base_connector import BaseConnector


class APIConnector(BaseConnector):
    """Connector for reading data from REST APIs"""
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the API connector.
        
        Args:
            spark: The SparkSession
            source_config: Configuration for the API source
            logger: Logger instance
        """
        super().__init__(spark, source_config, logger)
        
        # Extract API configuration
        self.url = self.source_config.get('url')
        self.method = self.source_config.get('method', 'GET')
        self.headers = self._resolve_env_vars_in_dict(self.source_config.get('headers', {}))
        self.params = self._resolve_env_vars_in_dict(self.source_config.get('params', {}))
        self.body = self._resolve_env_vars_in_dict(self.source_config.get('body', {}))
        self.response_format = self.source_config.get('response_format', 'json')
        self.timeout = self.source_config.get('timeout', 30)  # seconds
        
        # For paginated APIs
        self.pagination = self.source_config.get('pagination', {})
        self.is_paginated = bool(self.pagination)
    
    def _resolve_env_vars_in_dict(self, data: Dict) -> Dict:
        """
        Resolve environment variables in dictionary values.
        
        Args:
            data: Dictionary that may contain environment variable references like ${VAR_NAME}
            
        Returns:
            Dict: Dictionary with environment variables replaced with their values
        """
        if not data:
            return {}
            
        result = {}
        for key, value in data.items():
            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                env_var = value[2:-1]
                result[key] = os.environ.get(env_var, '')
            else:
                result[key] = value
        
        return result
    
    def _validate_source_config(self) -> None:
        """
        Validate API source configuration.
        
        Raises:
            ValueError: If any required configuration is missing
        """
        if not self.url:
            raise ValueError(f"URL must be provided for API source '{self.name}'")
        
        supported_methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']
        if self.method not in supported_methods:
            raise ValueError(
                f"Unsupported HTTP method '{self.method}' for source '{self.name}'. "
                f"Supported methods: {', '.join(supported_methods)}"
            )
        
        supported_formats = ['json', 'csv', 'xml']
        if self.response_format not in supported_formats:
            raise ValueError(
                f"Unsupported response format '{self.response_format}' for source '{self.name}'. "
                f"Supported formats: {', '.join(supported_formats)}"
            )
    
    def connect(self) -> bool:
        """
        Test the API connection.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            # Make a HEAD request to check if API is accessible
            test_method = 'HEAD' if self.method == 'GET' else self.method
            response = requests.request(
                method=test_method,
                url=self.url,
                headers=self.headers,
                params=self.params,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            self.logger.info(f"Successfully connected to API: {self.url}")
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error connecting to API '{self.url}': {str(e)}")
            return False
    
    def _make_request(self, url=None, params=None) -> Any:
        """
        Make an HTTP request to the API.
        
        Args:
            url: Override URL for pagination
            params: Override params for pagination
            
        Returns:
            Any: Response data
            
        Raises:
            Exception: If the request fails
        """
        try:
            actual_url = url or self.url
            actual_params = params or self.params
            
            self.logger.info(f"Making {self.method} request to {actual_url}")
            
            response = requests.request(
                method=self.method,
                url=actual_url,
                headers=self.headers,
                params=actual_params,
                json=self.body if self.method in ['POST', 'PUT', 'PATCH'] else None,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            if self.response_format == 'json':
                return response.json()
            elif self.response_format == 'csv':
                return response.text
            elif self.response_format == 'xml':
                return response.text
            else:
                return response.content
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            raise
    
    def _process_pagination(self) -> list:
        """
        Handle paginated API responses.
        
        Returns:
            list: Combined results from all pages
        """
        all_results = []
        page = 1
        max_pages = self.pagination.get('max_pages', 10)
        
        # Get pagination configuration
        page_param = self.pagination.get('page_param', 'page')
        next_page_link_path = self.pagination.get('next_page_link_path')
        has_more_path = self.pagination.get('has_more_path')
        results_path = self.pagination.get('results_path', '')
        
        params = self.params.copy()
        current_url = self.url
        
        while page <= max_pages:
            # Update pagination parameter if using page parameter
            if page_param and not next_page_link_path:
                params[page_param] = page
            
            # Make request
            response_data = self._make_request(url=current_url, params=params)
            
            # Extract results from response using results path
            page_results = response_data
            if results_path:
                for key in results_path.split('.'):
                    if isinstance(page_results, dict) and key in page_results:
                        page_results = page_results[key]
                    else:
                        self.logger.warning(f"Could not find results at path '{results_path}'")
                        page_results = []
                        break
            
            # Add results to accumulated list
            if isinstance(page_results, list):
                all_results.extend(page_results)
                self.logger.info(f"Retrieved {len(page_results)} results from page {page}")
            else:
                self.logger.warning(f"Expected list of results but got {type(page_results)}")
                break
            
            # Check if there are more pages
            has_more = True
            
            # If using next_page_link
            if next_page_link_path:
                next_url = response_data
                for key in next_page_link_path.split('.'):
                    if isinstance(next_url, dict) and key in next_url:
                        next_url = next_url[key]
                    else:
                        next_url = None
                        break
                
                if next_url:
                    current_url = next_url
                    params = {}  # Clear params as they are included in the next URL
                else:
                    has_more = False
            
            # If using has_more flag
            elif has_more_path:
                has_more_value = response_data
                for key in has_more_path.split('.'):
                    if isinstance(has_more_value, dict) and key in has_more_value:
                        has_more_value = has_more_value[key]
                    else:
                        has_more_value = False
                        break
                
                has_more = bool(has_more_value)
            
            # Stop if no more pages
            if not has_more:
                self.logger.info(f"No more pages to retrieve after page {page}")
                break
            
            page += 1
        
        return all_results
    
    def read_data(self) -> DataFrame:
        """
        Read data from the API into a Spark DataFrame.
        
        Returns:
            DataFrame: The data as a Spark DataFrame
        
        Raises:
            Exception: If there's an error reading from the API
        """
        try:
            # Get data from API
            if self.is_paginated:
                data = self._process_pagination()
            else:
                response_data = self._make_request()
                
                # For non-paginated APIs, results might be nested
                results_path = self.source_config.get('results_path', '')
                if results_path and isinstance(response_data, dict):
                    data = response_data
                    for key in results_path.split('.'):
                        if isinstance(data, dict) and key in data:
                            data = data[key]
                        else:
                            self.logger.warning(f"Could not find results at path '{results_path}'")
                            data = []
                            break
                else:
                    data = response_data
            
            # Convert data to Spark DataFrame
            if self.response_format == 'json':
                # For JSON data, we need to create a DataFrame from the Python objects
                json_str = json.dumps(data)
                df = self.spark.read.json(self.spark.sparkContext.parallelize([json_str]))
            elif self.response_format == 'csv':
                # For CSV, create a DataFrame from the text
                df = self.spark.read.csv(
                    self.spark.sparkContext.parallelize([data]),
                    header=True,
                    inferSchema=True
                )
            elif self.response_format == 'xml':
                # For XML, use the spark-xml package if available
                df = self.spark.read.format("com.databricks.spark.xml") \
                    .option("rowTag", self.source_config.get("xml_row_tag", "row")) \
                    .load(self.spark.sparkContext.parallelize([data]))
            else:
                raise ValueError(f"Unsupported response format: {self.response_format}")
            
            row_count = df.count()
            column_count = len(df.columns)
            self.logger.info(f"Successfully read {row_count} rows and {column_count} columns from API")
            
            return df
        
        except Exception as e:
            self.logger.error(f"Error reading data from API '{self.url}': {str(e)}")
            raise
    
    def get_source_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the API source.
        
        Returns:
            Dict[str, Any]: Source metadata
        """
        metadata = super().get_source_metadata()
        metadata.update({
            'url': self.url,
            'method': self.method,
            'response_format': self.response_format,
            'is_paginated': self.is_paginated
        })
        return metadata
