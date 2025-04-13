"""
Schema registry for managing data schemas, validation, and evolution.
Supports real-time, event-driven schema evolution.
"""

import os
import json
import yaml
import uuid
import logging
import threading
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union, Callable, Set
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, \
    DoubleType, BooleanType, TimestampType, DateType, ArrayType, DecimalType

class SchemaRegistry:
    """
    Registry for managing data schemas, versioning, validation, and evolution.
    """
    
    def __init__(
        self, 
        spark: SparkSession,
        schema_dir: str = "./schemas",
        logger: Optional[logging.Logger] = None,
        auto_evolution: bool = False,
        watch_interval: int = 30
    ):
        """
        Initialize the schema registry.
        
        Args:
            spark: The SparkSession
            schema_dir: Directory where schema definitions are stored
            logger: Logger instance
            auto_evolution: Whether to automatically evolve schemas based on data
            watch_interval: Interval in seconds to watch for schema changes
        """
        self.spark = spark
        self.schema_dir = schema_dir
        self.logger = logger or logging.getLogger(__name__)
        self.schemas = {}
        self.auto_evolution = auto_evolution
        self.watch_interval = watch_interval
        
        # Create schema directory if it doesn't exist
        os.makedirs(schema_dir, exist_ok=True)
        
        # Event listeners
        self.event_listeners = {
            'schema_updated': set(),
            'schema_evolved': set(),
            'schema_validated': set(),
            'evolution_requested': set()
        }
        
        # Schema change monitoring
        self.watch_thread = None
        self.watching = False
        self.schema_stats = {}
        
        # Load all schemas
        self._load_schemas()
        
        # Track schema stats for auto-evolution
        if auto_evolution:
            self._initialize_schema_stats()
    
    def _load_schemas(self):
        """
        Load all schemas from the schema directory.
        """
        if not os.path.exists(self.schema_dir):
            self.logger.warning(f"Schema directory not found: {self.schema_dir}")
            return
        
        for filename in os.listdir(self.schema_dir):
            if filename.endswith('.json') or filename.endswith('.yml') or filename.endswith('.yaml'):
                try:
                    schema_path = os.path.join(self.schema_dir, filename)
                    schema_name = os.path.splitext(filename)[0]
                    
                    # Load schema
                    with open(schema_path, 'r') as f:
                        if filename.endswith('.json'):
                            schema_def = json.load(f)
                        else:
                            schema_def = yaml.safe_load(f)
                    
                    # Store schema in registry
                    self.schemas[schema_name] = schema_def
                    self.logger.info(f"Loaded schema: {schema_name}")
                    
                except Exception as e:
                    self.logger.error(f"Error loading schema {filename}: {str(e)}")
    
    def get_schema(self, schema_name: str, version: str = 'latest') -> Dict[str, Any]:
        """
        Get a schema definition by name and version.
        
        Args:
            schema_name: Name of the schema
            version: Schema version, default is 'latest'
            
        Returns:
            Dict[str, Any]: Schema definition
            
        Raises:
            ValueError: If the schema or version is not found
        """
        if schema_name not in self.schemas:
            raise ValueError(f"Schema not found: {schema_name}")
        
        schema_def = self.schemas[schema_name]
        
        # Handle versioned schemas
        if 'versions' in schema_def:
            versions = schema_def['versions']
            
            if version == 'latest':
                # Get the latest version
                latest_version = max(versions.keys())
                return versions[latest_version]
            
            if version not in versions:
                raise ValueError(f"Version {version} not found for schema {schema_name}")
            
            return versions[version]
        
        # Non-versioned schema
        return schema_def
    
    def register_schema(self, schema_name: str, schema_def: Dict[str, Any], version: Optional[str] = None) -> None:
        """
        Register a new schema or version.
        
        Args:
            schema_name: Name of the schema
            schema_def: Schema definition
            version: Optional version string, if None, will create a new schema
            
        Raises:
            ValueError: If there's an issue with the schema definition
        """
        try:
            if schema_name in self.schemas and version:
                # Add a new version to existing schema
                if 'versions' not in self.schemas[schema_name]:
                    # Convert existing schema to versioned format
                    current_schema = self.schemas[schema_name]
                    self.schemas[schema_name] = {
                        'versions': {
                            'v1': current_schema
                        }
                    }
                
                # Add new version
                self.schemas[schema_name]['versions'][version] = schema_def
                
            else:
                # Create new schema
                self.schemas[schema_name] = schema_def
            
            # Save schema to file
            self._save_schema(schema_name)
            
            self.logger.info(f"Registered schema: {schema_name}" + (f" version {version}" if version else ""))
            
        except Exception as e:
            raise ValueError(f"Error registering schema {schema_name}: {str(e)}")
    
    def _save_schema(self, schema_name: str) -> None:
        """
        Save a schema to file.
        
        Args:
            schema_name: Name of the schema to save
        """
        try:
            schema_path = os.path.join(self.schema_dir, f"{schema_name}.json")
            
            with open(schema_path, 'w') as f:
                json.dump(self.schemas[schema_name], f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Error saving schema {schema_name}: {str(e)}")
    
    def to_spark_schema(self, schema_name: str, version: str = 'latest') -> StructType:
        """
        Convert a schema definition to a Spark StructType schema.
        
        Args:
            schema_name: Name of the schema
            version: Schema version, default is 'latest'
            
        Returns:
            StructType: Spark schema
            
        Raises:
            ValueError: If the schema or version is not found or invalid
        """
        schema_def = self.get_schema(schema_name, version)
        
        if 'fields' not in schema_def:
            raise ValueError(f"Invalid schema definition for {schema_name}: 'fields' key not found")
        
        return self._build_struct_type(schema_def['fields'])
    
    def _build_struct_type(self, fields: List[Dict[str, Any]]) -> StructType:
        """
        Build a Spark StructType from a list of field definitions.
        
        Args:
            fields: List of field definitions
            
        Returns:
            StructType: Spark schema
            
        Raises:
            ValueError: If a field definition is invalid
        """
        struct_fields = []
        
        for field in fields:
            name = field.get('name')
            data_type = field.get('type')
            nullable = field.get('nullable', True)
            
            if not name or not data_type:
                raise ValueError(f"Invalid field definition: {field}")
            
            # Convert to Spark data type
            spark_type = self._get_spark_type(data_type, field.get('fields'), field.get('element_type'))
            
            struct_fields.append(StructField(name, spark_type, nullable))
        
        return StructType(struct_fields)
    
    def _get_spark_type(self, data_type: str, nested_fields: Optional[List] = None, element_type: Optional[str] = None):
        """
        Get the corresponding Spark data type.
        
        Args:
            data_type: Type string
            nested_fields: Nested fields for struct types
            element_type: Element type for array types
            
        Returns:
            Spark data type
            
        Raises:
            ValueError: If the data type is not supported
        """
        data_type = data_type.lower()
        
        if data_type == 'string':
            return StringType()
        elif data_type == 'integer' or data_type == 'int':
            return IntegerType()
        elif data_type == 'float':
            return FloatType()
        elif data_type == 'double':
            return DoubleType()
        elif data_type == 'boolean' or data_type == 'bool':
            return BooleanType()
        elif data_type == 'timestamp':
            return TimestampType()
        elif data_type == 'date':
            return DateType()
        elif data_type == 'struct' and nested_fields:
            return self._build_struct_type(nested_fields)
        elif data_type == 'array' and element_type:
            return ArrayType(self._get_spark_type(element_type))
        elif data_type.startswith('decimal'):
            # Parse precision and scale from decimal(p,s)
            try:
                params = data_type.split('(')[1].rstrip(')').split(',')
                precision = int(params[0])
                scale = int(params[1]) if len(params) > 1 else 0
                return DecimalType(precision, scale)
            except Exception:
                return DecimalType(10, 2)  # Default precision and scale
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
    
    def validate_dataframe(self, df: DataFrame, schema_name: str, version: str = 'latest') -> Tuple[bool, Dict[str, Any]]:
        """
        Validate a DataFrame against a schema.
        
        Args:
            df: DataFrame to validate
            schema_name: Name of the schema to validate against
            version: Schema version, default is 'latest'
            
        Returns:
            Tuple[bool, Dict[str, Any]]: (is_valid, validation_results)
        """
        try:
            schema_def = self.get_schema(schema_name, version)
            validation_results = {
                'schema_name': schema_name,
                'version': version,
                'timestamp': datetime.now().isoformat(),
                'is_valid': True,
                'errors': [],
                'warnings': [],
                'metrics': {}
            }
            
            # Validate field existence and types
            expected_fields = {field['name']: field for field in schema_def.get('fields', [])}
            actual_fields = {field.name: field for field in df.schema.fields}
            
            # Check for missing fields
            missing_fields = [name for name in expected_fields.keys() if name not in actual_fields]
            for field_name in missing_fields:
                field_def = expected_fields[field_name]
                if not field_def.get('nullable', True):
                    validation_results['is_valid'] = False
                    validation_results['errors'].append(f"Required field missing: {field_name}")
                else:
                    validation_results['warnings'].append(f"Optional field missing: {field_name}")
            
            # Check for extra fields
            extra_fields = [name for name in actual_fields.keys() if name not in expected_fields]
            if extra_fields:
                validation_results['warnings'].append(f"Extra fields found: {', '.join(extra_fields)}")
            
            # Check field types
            for field_name, field_def in expected_fields.items():
                if field_name in actual_fields:
                    expected_type = field_def['type'].lower()
                    actual_type = str(actual_fields[field_name].dataType).lower()
                    
                    # Basic type compatibility check
                    if not self._are_types_compatible(expected_type, actual_type):
                        validation_results['is_valid'] = False
                        validation_results['errors'].append(
                            f"Type mismatch for field {field_name}: expected {expected_type}, got {actual_type}"
                        )
            
            # Validate pattern constraints if defined
            pattern_constraints = schema_def.get('pattern_constraints', {})
            if pattern_constraints:
                validation_results.update(self._validate_patterns(df, pattern_constraints))
            
            # Validate range constraints if defined
            range_constraints = schema_def.get('range_constraints', {})
            if range_constraints:
                validation_results.update(self._validate_ranges(df, range_constraints))
            
            # Calculate data quality metrics
            validation_results['metrics'] = self._calculate_validation_metrics(df, schema_def)
            
            return validation_results['is_valid'], validation_results
            
        except Exception as e:
            self.logger.error(f"Error validating DataFrame against schema {schema_name}: {str(e)}")
            return False, {
                'schema_name': schema_name,
                'version': version,
                'timestamp': datetime.now().isoformat(),
                'is_valid': False,
                'errors': [str(e)],
                'warnings': [],
                'metrics': {}
            }
    
    def _are_types_compatible(self, expected_type: str, actual_type: str) -> bool:
        """
        Check if two types are compatible.
        
        Args:
            expected_type: Expected type string
            actual_type: Actual type string
            
        Returns:
            bool: True if types are compatible
        """
        # Strip non-alphanumeric characters for comparison
        expected_type = ''.join(c for c in expected_type if c.isalnum()).lower()
        actual_type = ''.join(c for c in actual_type if c.isalnum()).lower()
        
        # Direct match
        if expected_type == actual_type:
            return True
        
        # Integer types
        if expected_type in ('int', 'integer') and actual_type in ('int', 'integer', 'bigint', 'smallint', 'tinyint'):
            return True
        
        # Floating point types
        if expected_type in ('float', 'double') and actual_type in ('float', 'double', 'decimal'):
            return True
        
        # String types
        if expected_type == 'string' and actual_type in ('string', 'varchar', 'char'):
            return True
        
        # Decimal types
        if expected_type.startswith('decimal') and actual_type.startswith('decimal'):
            return True
        
        return False
    
    def _validate_patterns(self, df: DataFrame, pattern_constraints: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate pattern constraints on DataFrame columns.
        
        Args:
            df: DataFrame to validate
            pattern_constraints: Dict mapping column names to regex patterns
            
        Returns:
            Dict[str, Any]: Validation results
        """
        results = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'pattern_validation': {}
        }
        
        for column, pattern in pattern_constraints.items():
            if column in df.columns:
                try:
                    # Count records that match/don't match the pattern
                    from pyspark.sql.functions import col, regexp_extract, count, when
                    
                    match_count = df.select(
                        count(when(regexp_extract(col(column), pattern, 0) != '', 1)).alias('match_count')
                    ).collect()[0]['match_count']
                    
                    total_count = df.count()
                    match_ratio = match_count / total_count if total_count > 0 else 0
                    
                    results['pattern_validation'][column] = {
                        'pattern': pattern,
                        'match_count': match_count,
                        'total_count': total_count,
                        'match_ratio': match_ratio
                    }
                    
                    # If less than 90% match, consider it a warning
                    if match_ratio < 0.9:
                        results['warnings'].append(
                            f"Pattern validation for column {column}: only {match_ratio:.2%} of values match pattern"
                        )
                    
                    # If less than 50% match, consider it an error
                    if match_ratio < 0.5:
                        results['is_valid'] = False
                        results['errors'].append(
                            f"Pattern validation failed for column {column}: only {match_ratio:.2%} of values match pattern"
                        )
                        
                except Exception as e:
                    results['warnings'].append(f"Error validating pattern for column {column}: {str(e)}")
            else:
                results['warnings'].append(f"Column {column} not found for pattern validation")
        
        return results
    
    def _validate_ranges(self, df: DataFrame, range_constraints: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate range constraints on DataFrame columns.
        
        Args:
            df: DataFrame to validate
            range_constraints: Dict mapping column names to range constraints
            
        Returns:
            Dict[str, Any]: Validation results
        """
        results = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'range_validation': {}
        }
        
        for column, constraints in range_constraints.items():
            if column in df.columns:
                try:
                    # Extract min and max values
                    from pyspark.sql.functions import min, max, col
                    
                    min_max = df.select(min(col(column)).alias('min_val'), max(col(column)).alias('max_val')).collect()[0]
                    min_val = min_max['min_val']
                    max_val = min_max['max_val']
                    
                    results['range_validation'][column] = {
                        'constraints': constraints,
                        'actual_min': min_val,
                        'actual_max': max_val,
                        'in_range': True
                    }
                    
                    # Check min constraint
                    if 'min' in constraints and min_val < constraints['min']:
                        results['range_validation'][column]['in_range'] = False
                        results['is_valid'] = False
                        results['errors'].append(
                            f"Range validation failed for column {column}: min value {min_val} is less than expected minimum {constraints['min']}"
                        )
                    
                    # Check max constraint
                    if 'max' in constraints and max_val > constraints['max']:
                        results['range_validation'][column]['in_range'] = False
                        results['is_valid'] = False
                        results['errors'].append(
                            f"Range validation failed for column {column}: max value {max_val} is greater than expected maximum {constraints['max']}"
                        )
                        
                except Exception as e:
                    results['warnings'].append(f"Error validating range for column {column}: {str(e)}")
            else:
                results['warnings'].append(f"Column {column} not found for range validation")
        
        return results
    
    def _calculate_validation_metrics(self, df: DataFrame, schema_def: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate validation metrics for a DataFrame.
        
        Args:
            df: DataFrame to analyze
            schema_def: Schema definition
            
        Returns:
            Dict[str, Any]: Validation metrics
        """
        from pyspark.sql.functions import col, count, isnan, when
        
        metrics = {
            'record_count': df.count(),
            'column_metrics': {}
        }
        
        # Calculate per-column metrics
        for field in schema_def.get('fields', []):
            column_name = field.get('name')
            
            if column_name in df.columns:
                # Count non-null values
                non_null_count = df.filter(col(column_name).isNotNull()).count()
                null_count = metrics['record_count'] - non_null_count
                
                # Create column metrics
                col_metrics = {
                    'null_count': null_count,
                    'non_null_count': non_null_count,
                    'completeness': non_null_count / metrics['record_count'] if metrics['record_count'] > 0 else 0
                }
                
                # Add type-specific metrics
                data_type = field.get('type', '').lower()
                
                if data_type in ('int', 'integer', 'float', 'double', 'decimal'):
                    # Count NaN values for numeric columns
                    nan_count = df.filter(isnan(col(column_name))).count()
                    col_metrics['nan_count'] = nan_count
                    
                    # Calculate numeric statistics
                    numeric_stats = df.select(
                        count(when(~isnan(col(column_name)) & col(column_name).isNotNull(), col(column_name))).alias('count'),
                        min(col(column_name)).alias('min'),
                        max(col(column_name)).alias('max')
                    ).collect()[0]
                    
                    col_metrics.update({
                        'min': numeric_stats['min'],
                        'max': numeric_stats['max'],
                    })
                
                elif data_type == 'string':
                    # Count empty strings
                    empty_count = df.filter((col(column_name) == '') & col(column_name).isNotNull()).count()
                    col_metrics['empty_count'] = empty_count
                    
                    # Count distinct values
                    distinct_count = df.select(column_name).distinct().count()
                    col_metrics['distinct_count'] = distinct_count
                    col_metrics['cardinality_ratio'] = distinct_count / non_null_count if non_null_count > 0 else 0
                
                metrics['column_metrics'][column_name] = col_metrics
        
        return metrics
    
    def _initialize_schema_stats(self):
        """Initialize schema statistics for tracking schema evolution metrics"""
        for schema_name in self.schemas:
            try:
                schema_def = self.get_schema(schema_name)
                self.schema_stats[schema_name] = {
                    'field_frequencies': {},
                    'pattern_violations': {},
                    'range_violations': {},
                    'evolution_history': [],
                    'last_updated': datetime.now().isoformat(),
                    'sample_data': {}
                }
                
                # Initialize field frequencies
                for field in schema_def.get('fields', []):
                    field_name = field['name']
                    self.schema_stats[schema_name]['field_frequencies'][field_name] = 1.0
            except Exception as e:
                self.logger.error(f"Error initializing stats for schema {schema_name}: {str(e)}")
    
    def add_event_listener(self, event_type: str, listener: Callable):
        """
        Add an event listener for schema events.
        
        Args:
            event_type: Type of event ('schema_updated', 'schema_evolved', 'schema_validated', 'evolution_requested')
            listener: Callback function to be called when event occurs
            
        Returns:
            str: Listener ID for removal
        """
        if event_type not in self.event_listeners:
            raise ValueError(f"Unknown event type: {event_type}")
        
        listener_id = str(uuid.uuid4())
        self.event_listeners[event_type].add((listener_id, listener))
        return listener_id
    
    def remove_event_listener(self, event_type: str, listener_id: str) -> bool:
        """
        Remove an event listener.
        
        Args:
            event_type: Type of event
            listener_id: ID of the listener to remove
            
        Returns:
            bool: True if listener was removed, False otherwise
        """
        if event_type not in self.event_listeners:
            return False
        
        for lid, listener in list(self.event_listeners[event_type]):
            if lid == listener_id:
                self.event_listeners[event_type].remove((lid, listener))
                return True
        
        return False
    
    def _emit_event(self, event_type: str, event_data: Dict[str, Any]):
        """
        Emit an event to all listeners of the given type.
        
        Args:
            event_type: Type of event
            event_data: Event data
        """
        if event_type not in self.event_listeners:
            return
        
        # Add timestamp and event type
        event_data['timestamp'] = datetime.now().isoformat()
        event_data['event_type'] = event_type
        
        # Call all listeners
        for _, listener in self.event_listeners[event_type]:
            try:
                listener(event_data)
            except Exception as e:
                self.logger.error(f"Error in event listener: {str(e)}")
    
    def start_schema_monitoring(self):
        """
        Start monitoring for schema changes.
        """
        if self.watching:
            return
        
        self.watching = True
        self.watch_thread = threading.Thread(target=self._monitor_schemas)
        self.watch_thread.daemon = True
        self.watch_thread.start()
        self.logger.info("Schema monitoring started")
    
    def stop_schema_monitoring(self):
        """
        Stop monitoring for schema changes.
        """
        self.watching = False
        if self.watch_thread:
            self.watch_thread.join(timeout=2)
            self.watch_thread = None
        self.logger.info("Schema monitoring stopped")
    
    def _monitor_schemas(self):
        """
        Monitor schemas for changes and trigger auto-evolution if needed.
        """
        while self.watching:
            try:
                # Check for schema files changes
                for filename in os.listdir(self.schema_dir):
                    if filename.endswith('.json') or filename.endswith('.yml') or filename.endswith('.yaml'):
                        schema_path = os.path.join(self.schema_dir, filename)
                        schema_name = os.path.splitext(filename)[0]
                        
                        # Check if file was modified
                        last_modified = os.path.getmtime(schema_path)
                        
                        # Skip if we've already processed this version
                        if schema_name in self.schemas and hasattr(self, '_last_modified') and schema_name in self._last_modified:
                            if last_modified <= self._last_modified[schema_name]:
                                continue
                        
                        # Reload schema
                        self.logger.info(f"Schema file changed: {schema_name}, reloading")
                        with open(schema_path, 'r') as f:
                            if filename.endswith('.json'):
                                schema_def = json.load(f)
                            else:
                                schema_def = yaml.safe_load(f)
                        
                        # Update schema
                        old_schema = self.schemas.get(schema_name)
                        self.schemas[schema_name] = schema_def
                        
                        # Track last modified time
                        if not hasattr(self, '_last_modified'):
                            self._last_modified = {}
                        self._last_modified[schema_name] = last_modified
                        
                        # Emit event
                        self._emit_event('schema_updated', {
                            'schema_name': schema_name,
                            'old_schema': old_schema,
                            'new_schema': schema_def
                        })
                
                # Sleep between checks
                time.sleep(self.watch_interval)
                
            except Exception as e:
                self.logger.error(f"Error in schema monitoring: {str(e)}")
                time.sleep(self.watch_interval * 2)  # Longer sleep on error
    
    def infer_schema_changes(self, df: DataFrame, schema_name: str, version: str = 'latest') -> Dict[str, Any]:
        """
        Infer potential schema changes based on a DataFrame.
        
        Args:
            df: DataFrame to analyze
            schema_name: Name of the schema
            version: Schema version
            
        Returns:
            Dict[str, Any]: Suggested schema changes
        """
        try:
            schema_def = self.get_schema(schema_name, version)
            expected_fields = {field['name']: field for field in schema_def.get('fields', [])}
            actual_fields = {field.name: field for field in df.schema.fields}
            
            changes = {
                'add_fields': [],
                'remove_fields': [],
                'modify_fields': [],
                'pattern_changes': [],
                'range_changes': []
            }
            
            # Find new fields
            for field_name, field in actual_fields.items():
                if field_name not in expected_fields:
                    field_type = str(field.dataType).split('(')[0].lower()
                    changes['add_fields'].append({
                        'name': field_name,
                        'type': field_type,
                        'nullable': field.nullable
                    })
            
            # Find removed fields
            for field_name in expected_fields:
                if field_name not in actual_fields:
                    changes['remove_fields'].append(field_name)
            
            # Find modified fields
            for field_name, field_def in expected_fields.items():
                if field_name in actual_fields:
                    expected_type = field_def['type'].lower()
                    actual_type = str(actual_fields[field_name].dataType).split('(')[0].lower()
                    
                    if not self._are_types_compatible(expected_type, actual_type):
                        changes['modify_fields'].append({
                            'name': field_name,
                            'old_type': expected_type,
                            'new_type': actual_type,
                            'nullable': actual_fields[field_name].nullable
                        })
            
            return changes
            
        except Exception as e:
            self.logger.error(f"Error inferring schema changes: {str(e)}")
            return {}
    
    def request_schema_evolution(self, schema_name: str, df: DataFrame) -> str:
        """
        Request schema evolution based on a DataFrame.
        
        Args:
            schema_name: Name of the schema
            df: DataFrame to base evolution on
            
        Returns:
            str: Request ID
        """
        # Generate request ID
        request_id = str(uuid.uuid4())
        
        # Generate schema changes
        changes = self.infer_schema_changes(df, schema_name)
        
        # Emit event
        self._emit_event('evolution_requested', {
            'schema_name': schema_name,
            'request_id': request_id,
            'changes': changes,
            'timestamp': datetime.now().isoformat()
        })
        
        return request_id
    
    def evolve_schema(self, schema_name: str, changes: Dict[str, Any], new_version: str) -> Dict[str, Any]:
        """
        Evolve a schema to a new version with specified changes.
        
        Args:
            schema_name: Name of the schema to evolve
            changes: Dictionary of changes to apply
            new_version: Version identifier for the new schema
            
        Returns:
            Dict[str, Any]: New schema definition
            
        Raises:
            ValueError: If the schema doesn't exist or there's an issue applying changes
        """
        if schema_name not in self.schemas:
            raise ValueError(f"Schema not found: {schema_name}")
        
        # Get the latest version of the schema
        current_schema = self.get_schema(schema_name)
        
        # Create a deep copy of the current schema
        import copy
        new_schema = copy.deepcopy(current_schema)
        
        # Apply changes
        try:
            # Add new fields
            if 'add_fields' in changes:
                for field in changes['add_fields']:
                    # Check if field already exists
                    field_names = [f['name'] for f in new_schema['fields']]
                    if field['name'] in field_names:
                        self.logger.warning(f"Field {field['name']} already exists in schema {schema_name}")
                    else:
                        new_schema['fields'].append(field)
            
            # Remove fields
            if 'remove_fields' in changes:
                for field_name in changes['remove_fields']:
                    new_schema['fields'] = [f for f in new_schema['fields'] if f['name'] != field_name]
            
            # Modify fields
            if 'modify_fields' in changes:
                for field_name, modifications in changes['modify_fields'].items():
                    for i, field in enumerate(new_schema['fields']):
                        if field['name'] == field_name:
                            # Apply modifications
                            for key, value in modifications.items():
                                if key == 'name':
                                    # Changing the field name
                                    new_schema['fields'][i]['name'] = value
                                else:
                                    # Other modifications
                                    new_schema['fields'][i][key] = value
            
            # Update pattern constraints
            if 'pattern_constraints' in changes:
                if 'pattern_constraints' not in new_schema:
                    new_schema['pattern_constraints'] = {}
                
                for column, pattern in changes['pattern_constraints'].items():
                    if pattern is None:
                        # Remove pattern constraint
                        if column in new_schema['pattern_constraints']:
                            del new_schema['pattern_constraints'][column]
                    else:
                        # Add or update pattern constraint
                        new_schema['pattern_constraints'][column] = pattern
            
            # Update range constraints
            if 'range_constraints' in changes:
                if 'range_constraints' not in new_schema:
                    new_schema['range_constraints'] = {}
                
                for column, constraints in changes['range_constraints'].items():
                    if constraints is None:
                        # Remove range constraint
                        if column in new_schema['range_constraints']:
                            del new_schema['range_constraints'][column]
                    else:
                        # Add or update range constraint
                        new_schema['range_constraints'][column] = constraints
            
            # Register the new schema version
            self.register_schema(schema_name, new_schema, new_version)
            
            return new_schema
            
        except Exception as e:
            raise ValueError(f"Error evolving schema {schema_name}: {str(e)}")
    
    def apply_schema_evolution(self, df: DataFrame, schema_name: str, from_version: str, to_version: str) -> DataFrame:
        """
        Apply schema evolution to a DataFrame.
        
        Args:
            df: DataFrame to evolve
            schema_name: Name of the schema
            from_version: Current schema version
            to_version: Target schema version
            
        Returns:
            DataFrame: Evolved DataFrame
            
        Raises:
            ValueError: If schema versions don't exist or evolution is not possible
        """
        # Get source and target schemas
        source_schema = self.get_schema(schema_name, from_version)
        target_schema = self.get_schema(schema_name, to_version)
        
        # Compare fields
        source_fields = {f['name']: f for f in source_schema.get('fields', [])}
        target_fields = {f['name']: f for f in target_schema.get('fields', [])}
        
        # Fields to add, remove, or modify
        fields_to_add = [f for name, f in target_fields.items() if name not in source_fields]
        fields_to_remove = [name for name in source_fields if name not in target_fields]
        fields_to_modify = {
            name: target_fields[name] 
            for name in source_fields 
            if name in target_fields and source_fields[name] != target_fields[name]
        }
        
        # Start with original DataFrame
        result_df = df
        
        # Remove columns
        for field_name in fields_to_remove:
            if field_name in result_df.columns:
                result_df = result_df.drop(field_name)
                self.logger.info(f"Removed column {field_name} during schema evolution")
        
        # Add new columns with default values
        from pyspark.sql.functions import lit
        
        for field in fields_to_add:
            field_name = field['name']
            field_type = field['type']
            default_value = field.get('default')
            
            if default_value is not None:
                # Add column with default value
                result_df = result_df.withColumn(field_name, lit(default_value))
                self.logger.info(f"Added column {field_name} with default value during schema evolution")
            else:
                # Add column with null value
                result_df = result_df.withColumn(field_name, lit(None))
                self.logger.info(f"Added column {field_name} with null value during schema evolution")
        
        # Modify columns
        for field_name, field_def in fields_to_modify.items():
            if field_name in result_df.columns:
                # For now, we just log that modification is needed
                # Actual type conversion or other modifications would require more complex logic
                self.logger.info(f"Column {field_name} needs modification during schema evolution")
                
                # If the field was renamed, handle it
                if 'previous_name' in field_def:
                    previous_name = field_def['previous_name']
                    if previous_name in result_df.columns:
                        result_df = result_df.withColumnRenamed(previous_name, field_name)
                        self.logger.info(f"Renamed column {previous_name} to {field_name} during schema evolution")
        
        return result_df

    def generate_schema_from_dataframe(self, df: DataFrame, schema_name: str) -> Dict[str, Any]:
        """
        Generate a schema definition from a DataFrame.
        
        Args:
            df: DataFrame to analyze
            schema_name: Name for the new schema
            
        Returns:
            Dict[str, Any]: Generated schema definition
        """
        fields = []
        
        for field in df.schema:
            field_def = {
                'name': field.name,
                'type': self._spark_type_to_string(field.dataType),
                'nullable': field.nullable
            }
            
            fields.append(field_def)
        
        schema_def = {
            'name': schema_name,
            'fields': fields,
            'created_at': datetime.now().isoformat()
        }
        
        # Register the schema
        self.register_schema(schema_name, schema_def)
        
        return schema_def
    
    def _spark_type_to_string(self, data_type) -> str:
        """
        Convert a Spark data type to a string representation.
        
        Args:
            data_type: Spark data type
            
        Returns:
            str: String representation of the data type
        """
        type_str = str(data_type)
        
        # Extract the type name without parameters
        if '(' in type_str:
            type_name = type_str.split('(')[0]
        else:
            type_name = type_str
        
        # Convert to schema registry type format
        type_map = {
            'StringType': 'string',
            'IntegerType': 'integer',
            'LongType': 'integer',
            'FloatType': 'float',
            'DoubleType': 'double',
            'BooleanType': 'boolean',
            'TimestampType': 'timestamp',
            'DateType': 'date',
            'DecimalType': type_str  # Keep parameters for decimal
        }
        
        return type_map.get(type_name, 'string')  # Default to string for unknown types