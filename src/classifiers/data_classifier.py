"""
Data classifier module for classifying data into bronze, silver, or gold categories
based on data quality metrics and schema validation.
"""

import os
import logging
from typing import Dict, Any, Optional, Tuple, List, Union
import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, isnan, when, isnull, to_timestamp, current_timestamp, lit, datediff

# Import schema registry
from src.utils.schema_registry import SchemaRegistry


class DataClassifier:
    """
    Classifier for determining data quality level (bronze, silver, gold)
    based on configurable metrics and schema validation.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: Dict[str, Any],
        default_classification: Dict[str, Dict[str, float]],
        logger: Optional[logging.Logger] = None,
        schema_dir: str = "./schemas"
    ):
        """
        Initialize the data classifier.
        
        Args:
            spark: The SparkSession
            source_config: Configuration for the data source
            default_classification: Default classification thresholds
            logger: Logger instance
            schema_dir: Directory where schema definitions are stored
        """
        self.spark = spark
        self.source_config = source_config
        self.default_classification = default_classification
        self.logger = logger or logging.getLogger(__name__)
        
        # Source-specific classification rules override defaults
        self.classification_rules = source_config.get('classification_rules', {})
        self.source_name = source_config.get('name', 'unnamed_source')
        
        # Initialize schema registry if schema directory exists
        self.schema_registry = None
        if os.path.exists(schema_dir):
            try:
                self.schema_registry = SchemaRegistry(spark, schema_dir, self.logger)
                self.logger.info(f"Schema registry initialized with {len(self.schema_registry.schemas)} schemas")
            except Exception as e:
                self.logger.warning(f"Could not initialize schema registry: {str(e)}")
    
    def _calculate_completeness(self, df: DataFrame) -> float:
        """
        Calculate data completeness (percentage of non-null values).
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            float: Completeness score between 0 and 1
        """
        if df.isEmpty():
            return 0.0
        
        # Count total number of cells
        total_cells = df.count() * len(df.columns)
        
        if total_cells == 0:
            return 0.0
        
        # Count null or NaN values across all columns
        null_counts = [df.filter(isnull(c) | isnan(c)).count() for c in df.columns]
        total_nulls = sum(null_counts)
        
        # Calculate completeness
        completeness = 1.0 - (total_nulls / total_cells)
        
        self.logger.info(f"Completeness score for {self.source_name}: {completeness:.4f}")
        return completeness
    
    def _calculate_accuracy(self, df: DataFrame) -> float:
        """
        Calculate data accuracy (percentage of values that match constraints).
        
        This is a simplified implementation that checks for values within expected ranges,
        matching patterns, or meeting other validation criteria.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            float: Accuracy score between 0 and 1
        """
        # This is a simplified implementation
        # In a real system, you would define specific validation rules for each column
        # based on business requirements
        
        if df.isEmpty():
            return 0.0
        
        # Get validation rules from source config
        validation_rules = self.source_config.get('validation_rules', {})
        
        # If no validation rules defined, use simplified accuracy as completeness
        if not validation_rules:
            self.logger.info(f"No validation rules defined for {self.source_name}, using completeness as accuracy")
            return self._calculate_completeness(df)
        
        # Count total number of cells
        total_cells = df.count() * len(df.columns)
        
        if total_cells == 0:
            return 0.0
        
        # Count invalid values based on rules
        invalid_count = 0
        
        # Apply validation rules to count invalid values
        for column, rules in validation_rules.items():
            if column not in df.columns:
                self.logger.warning(f"Column {column} not found in DataFrame")
                continue
            
            # Range validation
            if 'min' in rules and 'max' in rules:
                min_val = rules['min']
                max_val = rules['max']
                invalid_count += df.filter(~((col(column) >= min_val) & (col(column) <= max_val))).count()
            
            # Pattern validation (for string fields)
            if 'pattern' in rules:
                pattern = rules['pattern']
                invalid_count += df.filter(~col(column).rlike(pattern)).count()
            
            # Enumeration validation
            if 'allowed_values' in rules:
                allowed_values = rules['allowed_values']
                invalid_count += df.filter(~col(column).isin(allowed_values)).count()
        
        # Calculate accuracy
        accuracy = 1.0 - (invalid_count / total_cells)
        
        self.logger.info(f"Accuracy score for {self.source_name}: {accuracy:.4f}")
        return accuracy
    
    def _calculate_timeliness(self, df: DataFrame) -> float:
        """
        Calculate data timeliness (how recent the data is).
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            float: Timeliness score between 0 and 1
        """
        # Get date/timestamp column from source config
        datetime_column = self.source_config.get('datetime_column')
        
        # If no datetime column specified, use a simplified timeliness
        if not datetime_column or datetime_column not in df.columns:
            # Check if we're dealing with streaming data, which is always "fresh"
            if self.source_config.get('streaming', False):
                self.logger.info(f"Streaming data source {self.source_name}, using timeliness score 1.0")
                return 1.0
            
            self.logger.warning(f"No datetime column found for {self.source_name}, using default timeliness")
            return 0.5  # Neutral score
        
        try:
            # Calculate average age of data in days
            df_with_timestamp = df.withColumn(
                "timestamp_col", 
                to_timestamp(col(datetime_column))
            )
            
            # Calculate difference in days between data timestamp and current time
            df_with_age = df_with_timestamp.withColumn(
                "age_days",
                datediff(current_timestamp(), col("timestamp_col"))
            )
            
            # Get average age
            avg_age_days = df_with_age.selectExpr("avg(age_days)").first()[0]
            
            if avg_age_days is None:
                self.logger.warning(f"Could not calculate age for {self.source_name}, using default timeliness")
                return 0.5
            
            # Get timeliness threshold from rules (default: 30 days)
            timeliness_threshold = self.classification_rules.get('timeliness', 30)
            
            # Convert to score between 0 and 1 (fresher data = higher score)
            timeliness = max(0.0, min(1.0, 1.0 - (avg_age_days / timeliness_threshold)))
            
        except Exception as e:
            self.logger.error(f"Error calculating timeliness for {self.source_name}: {str(e)}")
            timeliness = 0.5  # Neutral score
        
        self.logger.info(f"Timeliness score for {self.source_name}: {timeliness:.4f}")
        return timeliness
    
    def validate_schema(self, df: DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate DataFrame against the source schema.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple[bool, Dict[str, Any]]: (is_valid, validation_results)
        """
        if not self.schema_registry:
            self.logger.warning("Schema validation skipped: Schema registry not initialized")
            return True, {'is_valid': True, 'errors': [], 'warnings': ["Schema validation skipped"]}
        
        # Determine schema name from source config
        source_name = self.source_name
        if not source_name:
            self.logger.warning("Schema validation skipped: Source name not found in configuration")
            return True, {'is_valid': True, 'errors': [], 'warnings': ["Source name not found"]}
        
        # Check if schema exists
        if source_name not in self.schema_registry.schemas:
            self.logger.warning(f"Schema validation skipped: Schema not found for source {source_name}")
            
            # Attempt to generate schema from DataFrame
            try:
                self.logger.info(f"Generating schema for {source_name} from DataFrame")
                schema_def = self.schema_registry.generate_schema_from_dataframe(df, source_name)
                self.logger.info(f"Schema generated for {source_name} with {len(schema_def['fields'])} fields")
                return True, {'is_valid': True, 'errors': [], 'warnings': ["Schema generated from DataFrame"]}
            except Exception as e:
                self.logger.error(f"Error generating schema for {source_name}: {str(e)}")
                return True, {'is_valid': True, 'errors': [], 'warnings': [f"Schema generation failed: {str(e)}"]}
        
        # Validate against schema
        try:
            # Get schema version from config or use latest
            schema_version = self.source_config.get('schema_version', 'latest')
            is_valid, validation_results = self.schema_registry.validate_dataframe(df, source_name, schema_version)
            
            # Log validation results
            if is_valid:
                self.logger.info(f"Schema validation passed for {source_name}")
            else:
                self.logger.warning(f"Schema validation failed for {source_name}: {validation_results['errors']}")
            
            return is_valid, validation_results
            
        except Exception as e:
            self.logger.error(f"Error during schema validation for {source_name}: {str(e)}")
            return False, {'is_valid': False, 'errors': [str(e)], 'warnings': [], 'metrics': {}}
            
    def apply_schema_evolution(self, df: DataFrame) -> DataFrame:
        """
        Apply schema evolution to a DataFrame if needed.
        
        Args:
            df: DataFrame to evolve
            
        Returns:
            DataFrame: Evolved DataFrame
        """
        if not self.schema_registry:
            self.logger.warning("Schema evolution skipped: Schema registry not initialized")
            return df
        
        # Determine schema name from source config
        source_name = self.source_name
        if not source_name or source_name not in self.schema_registry.schemas:
            return df
        
        # Check if schema evolution is needed
        current_version = self.source_config.get('schema_version', 'latest')
        target_version = self.source_config.get('target_schema_version')
        
        if not target_version or current_version == target_version:
            return df
        
        # Apply schema evolution
        try:
            self.logger.info(f"Applying schema evolution for {source_name} from {current_version} to {target_version}")
            evolved_df = self.schema_registry.apply_schema_evolution(df, source_name, current_version, target_version)
            self.logger.info(f"Schema evolution completed for {source_name}")
            return evolved_df
        except Exception as e:
            self.logger.error(f"Error applying schema evolution for {source_name}: {str(e)}")
            return df
    
    def calculate_metrics(self, df: DataFrame, validation_results: Optional[Dict[str, Any]] = None) -> Dict[str, Union[float, Dict[str, Any]]]:
        """
        Calculate all data quality metrics.
        
        Args:
            df: DataFrame to analyze
            validation_results: Optional schema validation results
            
        Returns:
            Dict[str, Union[float, Dict[str, Any]]]: Dictionary of metric names and scores
        """
        metrics = {
            'completeness': self._calculate_completeness(df),
            'accuracy': self._calculate_accuracy(df),
            'timeliness': self._calculate_timeliness(df)
        }
        
        # Add schema validation metrics if available
        if validation_results:
            metrics['schema_validation'] = {
                'is_valid': validation_results.get('is_valid', False),
                'error_count': len(validation_results.get('errors', [])),
                'warning_count': len(validation_results.get('warnings', [])),
                'validation_metrics': validation_results.get('metrics', {})
            }
        
        # Add additional custom metrics if needed
        # metrics['custom_metric'] = self._calculate_custom_metric(df)
        
        return metrics
    
    def classify_data(self, df: DataFrame) -> Tuple[str, Dict[str, Any]]:
        """
        Classify data as bronze, silver, or gold based on quality metrics
        and schema validation.
        
        Args:
            df: DataFrame to classify
            
        Returns:
            Tuple[str, Dict[str, Any]]: Tuple of (classification_level, metrics)
        """
        # First, apply schema evolution if needed
        evolved_df = self.apply_schema_evolution(df)
        
        # Validate schema
        is_schema_valid, validation_results = self.validate_schema(evolved_df)
        
        # Calculate metrics
        metrics = self.calculate_metrics(evolved_df, validation_results)
        
        # Get classification thresholds
        bronze_thresholds = self.default_classification.get('bronze', {})
        silver_thresholds = self.default_classification.get('silver', {})
        gold_thresholds = self.default_classification.get('gold', {})
        
        # Adjust classification criteria based on schema validation
        if not is_schema_valid:
            # If schema validation fails, the data can at most be classified as bronze
            is_gold = False
            is_silver = False
            
            # Check if metrics meet bronze criteria
            is_bronze = all(
                metrics.get(metric, 0) >= threshold
                for metric, threshold in bronze_thresholds.items()
                if isinstance(metrics.get(metric, 0), (int, float))  # Only compare numeric metrics
            )
        else:
            # Check if metrics meet gold criteria
            is_gold = all(
                metrics.get(metric, 0) >= threshold
                for metric, threshold in gold_thresholds.items()
                if isinstance(metrics.get(metric, 0), (int, float))  # Only compare numeric metrics
            )
            
            # Check if metrics meet silver criteria
            is_silver = all(
                metrics.get(metric, 0) >= threshold
                for metric, threshold in silver_thresholds.items()
                if isinstance(metrics.get(metric, 0), (int, float))  # Only compare numeric metrics
            )
            
            # Check if metrics meet bronze criteria
            is_bronze = all(
                metrics.get(metric, 0) >= threshold
                for metric, threshold in bronze_thresholds.items()
                if isinstance(metrics.get(metric, 0), (int, float))  # Only compare numeric metrics
            )
        
        # Determine classification
        if is_gold:
            classification = "gold"
        elif is_silver:
            classification = "silver"
        elif is_bronze:
            classification = "bronze"
        else:
            # Data doesn't meet even bronze criteria
            classification = "rejected"
        
        self.logger.info(f"Data from {self.source_name} classified as {classification.upper()}")
        self.logger.info(f"Metrics: {metrics}")
        
        return classification, metrics
    
    def add_classification_metadata(self, df: DataFrame, classification: str, metrics: Dict[str, Any]) -> DataFrame:
        """
        Add classification metadata columns to the DataFrame.
        
        Args:
            df: DataFrame to augment
            classification: Classification level (bronze, silver, gold, rejected)
            metrics: Calculated metrics
            
        Returns:
            DataFrame: DataFrame with additional metadata columns
        """
        # Add metadata columns
        result_df = df.withColumn("data_classification", lit(classification))
        
        # Add individual metrics (only the simple ones)
        for metric_name, metric_value in metrics.items():
            if isinstance(metric_value, (int, float)):
                result_df = result_df.withColumn(f"metric_{metric_name}", lit(metric_value))
        
        # Add schema validation metadata if available
        if 'schema_validation' in metrics:
            schema_validation = metrics['schema_validation']
            result_df = result_df.withColumn("schema_valid", lit(schema_validation.get('is_valid', False)))
            result_df = result_df.withColumn("schema_error_count", lit(schema_validation.get('error_count', 0)))
            result_df = result_df.withColumn("schema_warning_count", lit(schema_validation.get('warning_count', 0)))
        
        # Add processing timestamp
        result_df = result_df.withColumn("processing_timestamp", current_timestamp())
        
        # Add source name
        result_df = result_df.withColumn("source_name", lit(self.source_name))
        
        return result_df
