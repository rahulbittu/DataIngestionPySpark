"""
Elasticsearch connector for storing data and metrics in Elasticsearch.
"""

import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Union

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pyspark.sql import DataFrame

from src.connectors.base_connector import BaseConnector


class ElasticsearchConnector:
    """
    Connector for Elasticsearch to index data and metrics.
    Supports both document-level and bulk operations.
    """
    
    def __init__(
        self,
        es_config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the Elasticsearch connector.
        
        Args:
            es_config: Configuration for Elasticsearch
            logger: Logger instance
        """
        self.es_config = es_config
        self.logger = logger or logging.getLogger(__name__)
        self.client = None
        
    def connect(self) -> bool:
        """
        Connect to Elasticsearch cluster.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            hosts = self.es_config.get('hosts', ['http://localhost:9200'])
            username = self.es_config.get('username')
            password = self.es_config.get('password')
            verify_certs = self.es_config.get('verify_certs', True)
            ca_certs = self.es_config.get('ca_certs')
            api_key = self.es_config.get('api_key')
            
            # Build connection parameters
            conn_params = {
                'hosts': hosts,
                'verify_certs': verify_certs
            }
            
            # Add authentication if provided
            if username and password:
                conn_params['basic_auth'] = (username, password)
            elif api_key:
                conn_params['api_key'] = api_key
                
            # Add CA certificate if provided
            if ca_certs:
                conn_params['ca_certs'] = ca_certs
                
            # Create client
            self.client = Elasticsearch(**conn_params)
            
            # Test connection
            is_connected = self.client.ping()
            
            if is_connected:
                self.logger.info(f"Successfully connected to Elasticsearch at {hosts}")
                return True
            else:
                self.logger.error(f"Failed to connect to Elasticsearch at {hosts}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error connecting to Elasticsearch: {str(e)}")
            return False
            
    def create_index(
        self,
        index_name: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create an Elasticsearch index with optional mappings and settings.
        
        Args:
            index_name: Name of the index to create
            mappings: Optional index mappings
            settings: Optional index settings
            
        Returns:
            bool: True if index was created successfully, False otherwise
        """
        try:
            # Check if index exists
            if self.client.indices.exists(index=index_name):
                self.logger.info(f"Index {index_name} already exists")
                return True
                
            # Create index
            body = {}
            
            if mappings:
                body['mappings'] = mappings
                
            if settings:
                body['settings'] = settings
                
            response = self.client.indices.create(
                index=index_name,
                body=body if body else None
            )
            
            self.logger.info(f"Created index {index_name}: {response}")
            return response.get('acknowledged', False)
            
        except Exception as e:
            self.logger.error(f"Error creating index {index_name}: {str(e)}")
            return False
            
    def index_document(
        self, 
        index_name: str, 
        document: Dict[str, Any], 
        doc_id: Optional[str] = None
    ) -> bool:
        """
        Index a single document in Elasticsearch.
        
        Args:
            index_name: Name of the index
            document: Document to index
            doc_id: Optional document ID
            
        Returns:
            bool: True if document was indexed successfully, False otherwise
        """
        try:
            response = self.client.index(
                index=index_name,
                document=document,
                id=doc_id,
                refresh=True  # Force refresh so document is immediately searchable
            )
            
            result = response.get('result')
            is_success = result in ['created', 'updated']
            
            if is_success:
                self.logger.debug(f"Document indexed in {index_name}: {result}")
            else:
                self.logger.warning(f"Document index failed in {index_name}: {result}")
                
            return is_success
            
        except Exception as e:
            self.logger.error(f"Error indexing document in {index_name}: {str(e)}")
            return False
            
    def bulk_index(
        self,
        index_name: str,
        documents: List[Dict[str, Any]],
        id_field: Optional[str] = None
    ) -> Tuple[int, int]:
        """
        Bulk index multiple documents in Elasticsearch.
        
        Args:
            index_name: Name of the index
            documents: List of documents to index
            id_field: Optional field name to use as document ID
            
        Returns:
            Tuple[int, int]: Tuple of (success_count, error_count)
        """
        try:
            # Prepare bulk actions
            actions = []
            
            for doc in documents:
                action = {
                    "_index": index_name,
                    "_source": doc
                }
                
                # Add ID if requested
                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]
                    
                actions.append(action)
                
            # Execute bulk operation
            success, errors = bulk(
                self.client,
                actions,
                stats_only=True,
                refresh=True  # Force refresh so documents are immediately searchable
            )
            
            self.logger.info(f"Bulk indexed {success} documents in {index_name}, errors: {errors}")
            return (success, errors)
            
        except Exception as e:
            self.logger.error(f"Error bulk indexing documents in {index_name}: {str(e)}")
            return (0, len(documents))
            
    def delete_document(self, index_name: str, doc_id: str) -> bool:
        """
        Delete a document from Elasticsearch.
        
        Args:
            index_name: Name of the index
            doc_id: Document ID to delete
            
        Returns:
            bool: True if document was deleted successfully, False otherwise
        """
        try:
            response = self.client.delete(
                index=index_name,
                id=doc_id,
                refresh=True
            )
            
            result = response.get('result')
            is_success = result == 'deleted'
            
            if is_success:
                self.logger.debug(f"Document {doc_id} deleted from {index_name}")
            else:
                self.logger.warning(f"Document {doc_id} deletion failed in {index_name}: {result}")
                
            return is_success
            
        except Exception as e:
            self.logger.error(f"Error deleting document {doc_id} from {index_name}: {str(e)}")
            return False
            
    def search(
        self, 
        index_name: str, 
        query: Dict[str, Any],
        size: int = 10,
        from_: int = 0
    ) -> Dict[str, Any]:
        """
        Search for documents in Elasticsearch.
        
        Args:
            index_name: Name of the index to search
            query: Elasticsearch query DSL
            size: Maximum number of results to return
            from_: Starting offset for results
            
        Returns:
            Dict[str, Any]: Search results
        """
        try:
            response = self.client.search(
                index=index_name,
                body=query,
                size=size,
                from_=from_
            )
            
            return response
            
        except Exception as e:
            self.logger.error(f"Error searching index {index_name}: {str(e)}")
            return {"error": str(e)}
            
    def index_spark_dataframe(
        self,
        df: DataFrame,
        index_name: str,
        id_field: Optional[str] = None,
        batch_size: int = 1000
    ) -> Tuple[int, int]:
        """
        Index a Spark DataFrame in Elasticsearch.
        
        Args:
            df: Spark DataFrame to index
            index_name: Name of the index
            id_field: Optional field to use as document ID
            batch_size: Number of documents to index in each batch
            
        Returns:
            Tuple[int, int]: Tuple of (success_count, error_count)
        """
        try:
            # Convert DataFrame to list of dictionaries
            # Note: This operation collects data to the driver, so use batching for large DataFrames
            
            total_success = 0
            total_errors = 0
            total_documents = df.count()
            
            # Process in batches to avoid OOM on the driver
            for batch_df in df.repartition(max(10, df.rdd.getNumPartitions())).toLocalIterator():
                # Convert batch to dictionary
                batch_dict = batch_df.asDict(True)
                
                # Index batch
                success, errors = self.bulk_index(index_name, [batch_dict], id_field)
                
                total_success += success
                total_errors += errors
                
            self.logger.info(f"Indexed {total_success}/{total_documents} documents in {index_name}, errors: {total_errors}")
            return (total_success, total_errors)
            
        except Exception as e:
            self.logger.error(f"Error indexing DataFrame in {index_name}: {str(e)}")
            return (0, df.count())
            
    def index_metrics(
        self,
        metrics: Dict[str, Any],
        index_prefix: str = "metrics",
        include_timestamp: bool = True
    ) -> bool:
        """
        Index metrics data with automatic timestamping and daily indices.
        
        Args:
            metrics: Metrics data to index
            index_prefix: Prefix for the index name
            include_timestamp: Whether to add timestamp to the metrics
            
        Returns:
            bool: True if metrics were indexed successfully, False otherwise
        """
        try:
            # Create a copy to avoid modifying the original
            metrics_doc = {**metrics}
            
            # Add timestamp if requested
            if include_timestamp and '@timestamp' not in metrics_doc:
                metrics_doc['@timestamp'] = datetime.now().isoformat()
                
            # Create daily index name (e.g., metrics-2025.04.13)
            today = datetime.now().strftime('%Y.%m.%d')
            index_name = f"{index_prefix}-{today}"
            
            # Index document
            return self.index_document(index_name, metrics_doc)
            
        except Exception as e:
            self.logger.error(f"Error indexing metrics: {str(e)}")
            return False
            
    def close(self) -> None:
        """Close the Elasticsearch connection."""
        if self.client:
            self.client.close()
            self.logger.info("Elasticsearch connection closed")