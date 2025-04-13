"""
Flask web dashboard for monitoring the data ingestion pipeline.
"""

import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path
from threading import Thread
import time

from flask import Flask, render_template, jsonify, request
import pandas as pd

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config_loader import ConfigLoader
from src.utils.logging_utils import setup_logging

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET")

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

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


@app.route('/')
def index():
    """
    Render the dashboard home page.
    """
    return render_template('index.html', 
                          pipeline_metrics=pipeline_metrics,
                          source_status=source_status)


@app.route('/data_sources')
def data_sources():
    """
    Render the data sources page.
    """
    return render_template('data_sources.html', source_status=source_status)


@app.route('/monitoring')
def monitoring():
    """
    Render the monitoring page.
    """
    return render_template('monitoring.html', 
                          pipeline_metrics=pipeline_metrics,
                          source_status=source_status)


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
    mermaid_diagram = """
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
        
        %% Output
        E1 --> G[Transform Layer]
        E2 --> G
        E3 --> G
        
        %% Monitoring
        B --> H[Monitoring & Logging]
        D --> H
    """
    
    return jsonify({"diagram": mermaid_diagram})


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
            
            return jsonify({
                "config": source_config,
                "status": status
            })
    
    return jsonify({"error": "Source not found"}), 404


@app.template_filter('format_number')
def format_number(value):
    """
    Format number with thousands separator.
    """
    return f"{value:,}"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
