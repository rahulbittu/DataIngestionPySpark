"""
Flask web dashboard for monitoring the data ingestion pipeline.
Supports real-time updates through WebSockets.
"""

import os
import sys
import json
import logging
import time
import uuid
from datetime import datetime
from pathlib import Path
from threading import Thread, Lock
from collections import deque

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from flask import Flask, render_template, jsonify, request, session
import pandas as pd

from src.utils.config_loader import ConfigLoader
from src.utils.logging_utils import setup_logging

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", str(uuid.uuid4()))

# Initialize schema registry for schema validation if available
schema_registry = None

def init_schema_registry():
    """Initialize the schema registry if PySpark is available"""
    global schema_registry
    try:
        # Only import if needed
        from src.utils.schema_registry import SchemaRegistry
        
        # Check if schemas directory exists
        schemas_dir = "./schemas"
        if not os.path.exists(schemas_dir):
            logger.warning(f"Schemas directory not found: {schemas_dir}")
            return None
        
        # Check if schema files exist
        schema_files = [f for f in os.listdir(schemas_dir) if f.endswith(('.json', '.yml', '.yaml'))]
        if not schema_files:
            logger.warning(f"No schema files found in: {schemas_dir}")
            return None
        
        # Use mock SparkSession for web dashboard since we just need schema validation
        # without full Spark functionality
        class MockSparkSession:
            """Mock SparkSession for schema validation without full Spark"""
            def __init__(self):
                self.version = "3.4.0"
                
            def stop(self):
                pass
        
        mock_spark = MockSparkSession()
        schema_registry = SchemaRegistry(mock_spark, schema_dir=schemas_dir)
        logger.info(f"Schema registry initialized with {len(schema_registry.schemas)} schemas")
        return schema_registry
    except ImportError as e:
        logger.warning(f"Schema registry dependencies not available: {str(e)}")
        return None
    except Exception as e:
        logger.warning(f"Could not initialize schema registry: {str(e)}")
        return None

# Load configuration
try:
    config_path = './config/data_sources.yml'
    config_loader = ConfigLoader(config_path)
    logger.info(f"Configuration loaded successfully from {config_path}")
except Exception as e:
    logger.error(f"Error loading configuration: {str(e)}")
    logger.debug(f"Working directory: {os.getcwd()}")
    config_loader = None

# Store metrics in memory
pipeline_metrics = {
    'last_run': None,
    'sources_processed': 0,
    'sources_failed': 0,
    'records_processed': 0,
    'bronze_count': 0,
    'silver_count': 0,
    'gold_count': 0,
    'rejected_count': 0,
    'history': []
}

# Source status
source_status = []

# Schema validation status
schema_validations = []
column_validation_issues = []
pattern_matching_stats = []


def load_metrics_from_logs():
    """
    Load metrics from log files (if available).
    """
    try:
        metrics_log_file = "./logs/metrics.log"
        
        if os.path.exists(metrics_log_file):
            # Read last 1000 lines from metrics log
            with open(metrics_log_file, 'r') as f:
                lines = f.readlines()[-1000:]
                
            # Parse metrics from log lines
            for line in lines:
                if "Pipeline metrics:" in line:
                    # Extract metrics JSON
                    metrics_str = line.split("Pipeline metrics:")[1].strip()
                    try:
                        metrics = eval(metrics_str)
                        
                        # Update pipeline metrics
                        pipeline_metrics['last_run'] = datetime.fromtimestamp(metrics.get('end_time', 0)).strftime('%Y-%m-%d %H:%M:%S')
                        pipeline_metrics['sources_processed'] = metrics.get('sources_processed', 0)
                        pipeline_metrics['sources_failed'] = metrics.get('sources_failed', 0)
                        pipeline_metrics['records_processed'] = metrics.get('records_processed', 0)
                        pipeline_metrics['bronze_count'] = metrics.get('bronze_count', 0)
                        pipeline_metrics['silver_count'] = metrics.get('silver_count', 0)
                        pipeline_metrics['gold_count'] = metrics.get('gold_count', 0)
                        pipeline_metrics['rejected_count'] = metrics.get('rejected_count', 0)
                        
                        # Add to history
                        history_item = {
                            'timestamp': pipeline_metrics['last_run'],
                            'sources_processed': pipeline_metrics['sources_processed'],
                            'records_processed': pipeline_metrics['records_processed']
                        }
                        pipeline_metrics['history'].append(history_item)
                        
                        logger.info("Loaded metrics from logs")
                        
                    except Exception as e:
                        logger.error(f"Error parsing metrics from log: {str(e)}")
                
                elif "Source metrics:" in line:
                    # Extract source metrics JSON
                    metrics_str = line.split("Source metrics:")[1].strip()
                    try:
                        source_metrics = eval(metrics_str)
                        source_name = source_metrics.get('source_name')
                        source_type = source_metrics.get('source_type')
                        classification = source_metrics.get('classification')
                        
                        # Update source status
                        if source_name and source_type:
                            source_info = {
                                'name': source_name,
                                'type': source_type,
                                'last_run': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'status': 'success',
                                'classification': classification,
                                'record_count': source_metrics.get('record_count', 0)
                            }
                            
                            # Check if source already exists in status list
                            source_exists = False
                            for i, src in enumerate(source_status):
                                if src['name'] == source_name:
                                    source_status[i] = source_info
                                    source_exists = True
                                    break
                            
                            if not source_exists:
                                source_status.append(source_info)
                        
                    except Exception as e:
                        logger.error(f"Error parsing source metrics from log: {str(e)}")
        else:
            logger.warning(f"Metrics log file not found: {metrics_log_file}")
            
    except Exception as e:
        logger.error(f"Error loading metrics from logs: {str(e)}")


def load_source_configurations():
    """
    Load source configurations and add to source status.
    """
    if config_loader:
        try:
            all_sources = config_loader.get_all_sources()
            
            for source_type, sources in all_sources.items():
                type_name = source_type.replace('_sources', '')
                
                for source in sources:
                    source_name = source.get('name')
                    
                    if source_name:
                        # Check if source already exists in status list
                        source_exists = False
                        for src in source_status:
                            if src['name'] == source_name:
                                source_exists = True
                                break
                        
                        if not source_exists:
                            source_info = {
                                'name': source_name,
                                'type': type_name,
                                'last_run': 'Never',
                                'status': 'pending',
                                'classification': 'N/A',
                                'record_count': 0
                            }
                            source_status.append(source_info)
            
            logger.info(f"Loaded {len(source_status)} source configurations")
            
        except Exception as e:
            logger.error(f"Error loading source configurations: {str(e)}")


# Initialize data
load_source_configurations()
load_metrics_from_logs()

# Try to initialize schema registry (but don't stop if it fails)
try:
    schema_registry = init_schema_registry()
except Exception as e:
    logger.warning(f"Error initializing schema registry: {str(e)}")


@app.route('/')
def index():
    """
    Render the dashboard home page.
    """
    # Check if Hive is configured (primary storage)
    hive_enabled = False
    hive_connection_info = {}
    
    if config_loader:
        hive_config = config_loader.get_hive_config()
        if hive_config:
            hive_enabled = True
            hive_connection_info = {
                'database': hive_config.get('database', 'default'),
                'host': hive_config.get('host', 'localhost'),
                'port': hive_config.get('port', 10000),
                'table_prefix': hive_config.get('table_prefix', '')
            }
    
    # Check if Elasticsearch is configured (secondary storage)
    elasticsearch_enabled = False
    elasticsearch_kibana_url = "http://localhost:5601"  # Default Kibana URL
    
    if config_loader:
        es_config = config_loader.get_elasticsearch_config()
        if es_config:
            elasticsearch_enabled = True
            # Try to extract Kibana URL from config if available
            if 'kibana_url' in es_config:
                elasticsearch_kibana_url = es_config['kibana_url']
    
    return render_template('index.html', 
                          pipeline_metrics=pipeline_metrics,
                          source_status=source_status,
                          hive_enabled=hive_enabled,
                          hive_connection_info=hive_connection_info,
                          elasticsearch_enabled=elasticsearch_enabled,
                          elasticsearch_kibana_url=elasticsearch_kibana_url)


@app.route('/data_sources')
def data_sources():
    """
    Render the data sources page.
    """
    # Check if Elasticsearch is configured
    elasticsearch_enabled = False
    elasticsearch_kibana_url = "http://localhost:5601"  # Default Kibana URL
    
    if config_loader:
        es_config = config_loader.get_elasticsearch_config()
        if es_config:
            elasticsearch_enabled = True
            if 'kibana_url' in es_config:
                elasticsearch_kibana_url = es_config['kibana_url']
    
    return render_template('data_sources.html', 
                          source_status=source_status,
                          elasticsearch_enabled=elasticsearch_enabled,
                          elasticsearch_kibana_url=elasticsearch_kibana_url)


@app.route('/monitoring')
def monitoring():
    """
    Render the monitoring page.
    """
    # Check if Elasticsearch is configured
    elasticsearch_enabled = False
    elasticsearch_kibana_url = "http://localhost:5601"  # Default Kibana URL
    
    if config_loader:
        es_config = config_loader.get_elasticsearch_config()
        if es_config:
            elasticsearch_enabled = True
            if 'kibana_url' in es_config:
                elasticsearch_kibana_url = es_config['kibana_url']
    
    return render_template('monitoring.html', 
                          pipeline_metrics=pipeline_metrics,
                          source_status=source_status,
                          elasticsearch_enabled=elasticsearch_enabled,
                          elasticsearch_kibana_url=elasticsearch_kibana_url)


@app.route('/api/metrics')
def api_metrics():
    """
    API endpoint for getting pipeline metrics.
    """
    return jsonify(pipeline_metrics)


@app.route('/api/sources')
def api_sources():
    """
    API endpoint for getting source status.
    """
    return jsonify(source_status)


@app.route('/api/diagram')
def api_diagram():
    """
    API endpoint for getting mermaid diagram.
    """
    # Check if Hive is configured (primary storage)
    hive_enabled = False
    if config_loader:
        hive_config = config_loader.get_hive_config()
        if hive_config:
            hive_enabled = True
            
    # Check if Elasticsearch is configured (secondary storage)
    elasticsearch_enabled = False
    if config_loader:
        es_config = config_loader.get_elasticsearch_config()
        if es_config:
            elasticsearch_enabled = True
    
    # Create a simple black and white data flow diagram
    mermaid_diagram = """
    %%{init: {'theme': 'base', 'themeVariables': { 'primaryColor': '#ffffff', 'primaryTextColor': '#000000', 'primaryBorderColor': '#000000', 'lineColor': '#ffffff', 'secondaryColor': '#ffffff', 'tertiaryColor': '#ffffff' }}}%%
    graph TD
        A[Data Sources] --> B[Ingestion Layer]
        
        %% Data Sources
        A --> C1[File Sources]
        A --> C2[Database Sources]
        A --> C3[API Sources]
        A --> C4[Kafka Sources]
        
        %% Ingestion Process
        B --> D[Data Classification]
        D --> E1[Bronze]
        D --> E2[Silver]
        D --> E3[Gold]
        D --> E4[Rejected]
        
        %% Classification Rules
        F[Classification Rules] --> D
        F1[Completeness] --> F
        F2[Accuracy] --> F
        F3[Timeliness] --> F
        
        %% Storage Options
        E1 --> S1[Storage Layer]
        E2 --> S1
        E3 --> S1
        E4 --> S1
        
        %% Conditional Rendering for Hive
        S1 --> H[Hive Tables]
        S1 --> ES[Elasticsearch]
        
        %% Monitoring
        B --> M[Metrics]
        D --> M
        M --> L[Monitoring Dashboard]
    """
    
    return jsonify({"diagram": mermaid_diagram})


@app.route('/schema_validation')
def schema_validation():
    """
    Render the schema validation page.
    """
    # Generate schema stats 
    valid_count = sum(1 for v in schema_validations if v.get('is_valid', False))
    total_count = len(schema_validations) if schema_validations else 0
    valid_percentage = int(valid_count / total_count * 100) if total_count > 0 else 0
    
    schema_stats = {
        'valid_count': valid_count,
        'invalid_count': total_count - valid_count,
        'total_count': total_count,
        'valid_percentage': valid_percentage
    }
    
    # Get schema versions
    schema_versions = []
    if schema_registry:
        for schema_name, schema_def in schema_registry.schemas.items():
            if 'versions' in schema_def:
                latest_version = max(schema_def['versions'].keys())
                current_version = 'latest'  # This could come from source config
                schema_versions.append({
                    'name': schema_name,
                    'current_version': current_version,
                    'latest_version': latest_version
                })
            else:
                schema_versions.append({
                    'name': schema_name,
                    'current_version': 'v1',
                    'latest_version': 'v1'
                })
    
    # Check if Elasticsearch is configured
    elasticsearch_enabled = False
    elasticsearch_kibana_url = "http://localhost:5601"  # Default Kibana URL
    
    if config_loader:
        es_config = config_loader.get_elasticsearch_config()
        if es_config:
            elasticsearch_enabled = True
            if 'kibana_url' in es_config:
                elasticsearch_kibana_url = es_config['kibana_url']
                
    return render_template('schema_validation.html',
                          schema_stats=schema_stats,
                          schema_versions=schema_versions,
                          schema_validations=schema_validations,
                          column_validation_issues=column_validation_issues,
                          pattern_matching_stats=pattern_matching_stats,
                          elasticsearch_enabled=elasticsearch_enabled,
                          elasticsearch_kibana_url=elasticsearch_kibana_url)


@app.route('/api/schema/<schema_name>')
def api_schema_details(schema_name):
    """
    API endpoint for getting details about a specific schema.
    """
    if schema_registry and schema_name in schema_registry.schemas:
        schema_def = schema_registry.schemas[schema_name]
        
        # For versioned schemas, return the latest version by default
        if 'versions' in schema_def:
            latest_version = max(schema_def['versions'].keys())
            schema = schema_def['versions'][latest_version]
        else:
            schema = schema_def
        
        # Get validation results if available
        validation_results = None
        for validation in schema_validations:
            if validation.get('name') == schema_name:
                validation_results = validation
                break
        
        return jsonify({
            "schema": schema,
            "validation_results": validation_results
        })
    
    return jsonify({"error": "Schema not found"}), 404


@app.route('/api/schemas')
def api_schemas():
    """
    API endpoint for getting all schemas.
    """
    if schema_registry:
        schemas = []
        for name, schema_def in schema_registry.schemas.items():
            # For versioned schemas, return version info
            if 'versions' in schema_def:
                versions = list(schema_def['versions'].keys())
                latest_version = max(versions)
                schema_info = {
                    'name': name,
                    'versions': versions,
                    'latest_version': latest_version
                }
            else:
                schema_info = {
                    'name': name,
                    'versions': ['v1'],
                    'latest_version': 'v1'
                }
            schemas.append(schema_info)
        
        return jsonify(schemas)
    
    return jsonify([])


@app.route('/api/source/<source_name>')
def api_source_details(source_name):
    """
    API endpoint for getting details about a specific source.
    """
    if config_loader:
        source_config = config_loader.get_source_by_name(source_name)
        
        if source_config:
            # Find source status
            status = None
            for src in source_status:
                if src['name'] == source_name:
                    status = src
                    break
            
            # Get schema validation info if available
            schema_validation = None
            if schema_registry and source_name in schema_registry.schemas:
                for validation in schema_validations:
                    if validation.get('name') == source_name:
                        schema_validation = validation
                        break
            
            return jsonify({
                "config": source_config,
                "status": status,
                "schema_validation": schema_validation
            })
    
    return jsonify({"error": "Source not found"}), 404


# Real-time event queue for storing latest events
event_queue = deque(maxlen=100)
metrics_lock = Lock()  # Lock for thread-safe metrics updates

# SocketIO event handlers
# Remove WebSocket event handlers

def update_metrics(new_metrics):
    """
    Update pipeline metrics with thread safety.
    
    Args:
        new_metrics: New metrics to update
    """
    with metrics_lock:
        # Update metrics
        pipeline_metrics.update(new_metrics)
        
        # Add to history if timestamp is provided
        if 'last_run' in new_metrics:
            history_item = {
                'timestamp': new_metrics['last_run'],
                'sources_processed': pipeline_metrics['sources_processed'],
                'records_processed': pipeline_metrics['records_processed']
            }
            pipeline_metrics['history'].append(history_item)

def update_source_status(source_name, status_update):
    """
    Update source status with thread safety.
    
    Args:
        source_name: Name of the source
        status_update: Status update data
    """
    updated = False
    
    # Find and update the source
    for i, source in enumerate(source_status):
        if source['name'] == source_name:
            source_status[i].update(status_update)
            updated = True
            break
    
    # Add new source if not found
    if not updated:
        new_source = {'name': source_name}
        new_source.update(status_update)
        source_status.append(new_source)

def publish_event(event_data):
    """
    Add an event to the queue.
    
    Args:
        event_data: Event data to publish
    """
    # Add timestamp if not present
    if 'timestamp' not in event_data:
        event_data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Add event to queue
    event_queue.append(event_data)

@app.template_filter('format_number')
def format_number(value):
    """
    Format number with thousands separator.
    """
    return f"{value:,}"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
