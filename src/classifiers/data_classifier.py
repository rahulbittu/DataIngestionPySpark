"""
Data classifier module for classifying data into bronze, silver, or gold categories
based on data quality metrics.
"""

import logging
from typing import Dict, Any, Optional, Tuple, List
import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, isnan, when, isnull, to_timestamp, current_timestamp, lit, datediff


class DataClassifier:
    """
    Classifier for determining data quality level (bronze, silver, gold)
    based on configurable metrics.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: Dict[str, Any],
        default_classification: Dict[str, Dict[str, float]],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the data classifier.
        
        Args:
            spark: The SparkSession
            source_config: Configuration for the data source
            default_classification: Default classification thresholds
            logger: Logger instance
        """
        self.spark = spark
        self.source_config = source_config
        self.default_classification = default_classification
        self.logger = logger or logging.getLogger(__name__)
        
        # Source-specific classification rules override defaults
        self.classification_rules = source_config.get('classification_rules', {})
        self.source_name = source_config.get('name', 'unnamed_source')
    
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
    
    def calculate_metrics(self, df: DataFrame) -> Dict[str, float]:
        """
        Calculate all data quality metrics.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dict[str, float]: Dictionary of metric names and scores
        """
        metrics = {
            'completeness': self._calculate_completeness(df),
            'accuracy': self._calculate_accuracy(df),
            'timeliness': self._calculate_timeliness(df)
        }
        
        # Add additional custom metrics if needed
        # metrics['custom_metric'] = self._calculate_custom_metric(df)
        
        return metrics
    
    def classify_data(self, df: DataFrame) -> Tuple[str, Dict[str, float]]:
        """
        Classify data as bronze, silver, or gold based on quality metrics.
        
        Args:
            df: DataFrame to classify
            
        Returns:
            Tuple[str, Dict[str, float]]: Tuple of (classification_level, metrics)
        """
        metrics = self.calculate_metrics(df)
        
        # Get classification thresholds
        bronze_thresholds = self.default_classification.get('bronze', {})
        silver_thresholds = self.default_classification.get('silver', {})
        gold_thresholds = self.default_classification.get('gold', {})
        
        # Check if metrics meet gold criteria
        is_gold = all(
            metrics.get(metric, 0) >= threshold
            for metric, threshold in gold_thresholds.items()
        )
        
        # Check if metrics meet silver criteria
        is_silver = all(
            metrics.get(metric, 0) >= threshold
            for metric, threshold in silver_thresholds.items()
        )
        
        # Check if metrics meet bronze criteria
        is_bronze = all(
            metrics.get(metric, 0) >= threshold
            for metric, threshold in bronze_thresholds.items()
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
    
    def add_classification_metadata(self, df: DataFrame, classification: str, metrics: Dict[str, float]) -> DataFrame:
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
        
        # Add individual metrics
        for metric_name, metric_value in metrics.items():
            result_df = result_df.withColumn(f"metric_{metric_name}", lit(metric_value))
        
        # Add processing timestamp
        result_df = result_df.withColumn("processing_timestamp", current_timestamp())
        
        # Add source name
        result_df = result_df.withColumn("source_name", lit(self.source_name))
        
        return result_df
