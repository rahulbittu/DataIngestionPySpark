"""
NVD Connector for fetching Common Vulnerabilities and Exposures (CVE) data.
This connector fetches data from the NVD API and converts it to a Spark DataFrame.
"""
import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, IntegerType, FloatType

from .base_connector import BaseConnector


class NVDConnector(BaseConnector):
    """Connector for reading CVE data from the National Vulnerability Database (NVD) API"""

    # Base URL for NVD API
    _NVD_API_BASE_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0"
    
    # Default schema for CVE data
    _CVE_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("sourceIdentifier", StringType(), True),
        StructField("published", StringType(), True),
        StructField("lastModified", StringType(), True),
        StructField("vulnStatus", StringType(), True),
        StructField("descriptions", ArrayType(
            StructType([
                StructField("lang", StringType(), True),
                StructField("value", StringType(), True)
            ])
        ), True),
        StructField("metrics", MapType(StringType(), 
            StructType([
                StructField("cvssData", StructType([
                    StructField("version", StringType(), True),
                    StructField("vectorString", StringType(), True),
                    StructField("baseScore", FloatType(), True),
                    StructField("baseSeverity", StringType(), True)
                ]), True),
                StructField("exploitabilityScore", FloatType(), True),
                StructField("impactScore", FloatType(), True)
            ])
        ), True),
        StructField("weaknesses", ArrayType(
            StructType([
                StructField("source", StringType(), True),
                StructField("type", StringType(), True),
                StructField("description", ArrayType(
                    StructType([
                        StructField("lang", StringType(), True),
                        StructField("value", StringType(), True)
                    ])
                ), True)
            ])
        ), True),
        StructField("configurations", ArrayType(
            StructType([
                StructField("nodes", ArrayType(
                    StructType([
                        StructField("operator", StringType(), True),
                        StructField("negate", StringType(), True),
                        StructField("cpeMatch", ArrayType(
                            StructType([
                                StructField("vulnerable", StringType(), True),
                                StructField("criteria", StringType(), True),
                                StructField("matchCriteriaId", StringType(), True)
                            ])
                        ), True)
                    ])
                ), True)
            ])
        ), True),
        StructField("references", ArrayType(
            StructType([
                StructField("url", StringType(), True),
                StructField("source", StringType(), True),
                StructField("tags", ArrayType(StringType()), True)
            ])
        ), True)
    ])

    def __init__(
        self,
        spark: SparkSession,
        source_config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the NVD connector.
        
        Args:
            spark: The SparkSession
            source_config: Configuration for the NVD source
            logger: Logger instance
        """
        super().__init__(spark, source_config, logger)
        
        # Extract API key if provided
        self.api_key = self.source_config.get('api_key')
        self.delay_between_requests = self.source_config.get('delay_between_requests', 6)
        self.results_per_page = self.source_config.get('results_per_page', 2000)
        self.max_pages = self.source_config.get('max_pages')
        self.start_date = self.source_config.get('start_date')
        self.end_date = self.source_config.get('end_date')
        self.additional_query_params = self.source_config.get('additional_query_params', {})
        
        # Setup headers if API key is provided
        self.headers = {}
        if self.api_key:
            self.headers["apiKey"] = self.api_key

    def _validate_source_config(self) -> None:
        """
        Validate NVD source configuration.
        
        Raises:
            ValueError: If any required configuration is missing
        """
        required_fields = []
        for field in required_fields:
            if field not in self.source_config:
                raise ValueError(f"Required field '{field}' not found in NVD source configuration")

    def connect(self) -> bool:
        """
        Test the connection to the NVD API.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            # Send a minimal request to test connection
            response = requests.get(
                self._NVD_API_BASE_URL,
                headers=self.headers,
                params={"resultsPerPage": 1}
            )
            
            if response.status_code == 200:
                self.logger.info("Successfully connected to NVD API")
                
                # Check if the request rate limit is close
                remaining = response.headers.get("X-RateLimit-Remaining")
                if remaining and int(remaining) < 10:
                    self.logger.warning(f"NVD API rate limit nearly reached. Remaining: {remaining}")
                    
                return True
            elif response.status_code == 403:
                self.logger.error("Authentication failed. Check your API key.")
                return False
            elif response.status_code == 429:
                self.logger.error("Rate limit exceeded. Please wait before making more requests.")
                return False
            else:
                self.logger.error(f"Failed to connect to NVD API. Status code: {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                return False
        except Exception as e:
            self.logger.exception(f"Error connecting to NVD API: {str(e)}")
            return False

    def read_data(self) -> DataFrame:
        """
        Read CVE data from NVD API into a Spark DataFrame.
        
        Returns:
            DataFrame: The CVE data as a Spark DataFrame
        
        Raises:
            Exception: If there's an error reading from the API
        """
        self.logger.info("Fetching CVE data from NVD API")
        
        try:
            # Build base query parameters
            params = {
                "resultsPerPage": self.results_per_page
            }
            
            # Add date range if specified
            if self.start_date:
                params["pubStartDate"] = self.start_date
            
            if self.end_date:
                params["pubEndDate"] = self.end_date
                
            # Add any additional parameters
            params.update(self.additional_query_params)
            
            # Initialize variables for pagination
            all_cves = []
            current_page = 0
            total_results = None
            
            # Process the first page and get the total number of results
            response = self._fetch_page(params)
            if response:
                result_data = response.json()
                total_results = result_data.get("totalResults", 0)
                self.logger.info(f"Total CVEs to fetch: {total_results}")
                
                # Extract CVEs from the response
                cves = result_data.get("vulnerabilities", [])
                all_cves.extend([item.get("cve", {}) for item in cves])
                
                # Check if there are more pages to fetch
                has_more = len(all_cves) < total_results
                current_page += 1
                
                # Fetch remaining pages
                while has_more and (self.max_pages is None or current_page < self.max_pages):
                    # Add startIndex for pagination
                    params["startIndex"] = current_page * self.results_per_page
                    
                    # Delay to respect rate limits
                    time.sleep(self.delay_between_requests)
                    
                    # Fetch the next page
                    self.logger.info(f"Fetching page {current_page + 1} of CVE data")
                    response = self._fetch_page(params)
                    
                    if response:
                        result_data = response.json()
                        cves = result_data.get("vulnerabilities", [])
                        all_cves.extend([item.get("cve", {}) for item in cves])
                        
                        # Update pagination state
                        has_more = len(all_cves) < total_results
                        current_page += 1
                    else:
                        # If we encounter an error, stop pagination
                        has_more = False
            
            # Convert the raw data to a Spark DataFrame
            self.logger.info(f"Total CVEs fetched: {len(all_cves)}")
            
            if not all_cves:
                self.logger.warning("No CVE data fetched from NVD API")
                return self.spark.createDataFrame([], self._CVE_SCHEMA)
            
            # Create DataFrame from the collected data
            cve_df = self.spark.createDataFrame(all_cves)
            
            return cve_df
            
        except Exception as e:
            self.logger.exception(f"Error reading data from NVD API: {str(e)}")
            raise

    def _fetch_page(self, params: Dict[str, Any]) -> Optional[requests.Response]:
        """
        Fetch a single page of data from the NVD API.
        
        Args:
            params: Query parameters
            
        Returns:
            Response object if successful, None otherwise
        """
        try:
            response = requests.get(
                self._NVD_API_BASE_URL,
                headers=self.headers,
                params=params
            )
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                self.logger.warning("Rate limit exceeded. Waiting before retrying...")
                time.sleep(30)  # Wait longer for rate limit cooldown
                return None
            else:
                self.logger.error(f"Failed to fetch CVE data. Status code: {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                return None
                
        except Exception as e:
            self.logger.exception(f"Error fetching page from NVD API: {str(e)}")
            return None

    def get_source_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the NVD source.
        
        Returns:
            Dict[str, Any]: Source metadata
        """
        return {
            "name": self.source_config.get("name", "nvd"),
            "type": "api",
            "subtype": "nvd",
            "description": "National Vulnerability Database CVE data",
            "start_date": self.start_date,
            "end_date": self.end_date,
            "connection_status": "connected" if self.connect() else "disconnected"
        }