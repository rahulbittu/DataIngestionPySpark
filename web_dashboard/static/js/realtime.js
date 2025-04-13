/**
 * Real-time WebSocket connection and event handling for the dashboard
 */

// Initialize SocketIO connection and event handlers
document.addEventListener('DOMContentLoaded', function() {
    // Initialize socket connection
    initializeSocket();
    
    // Set up event filter handlers
    setupEventFilters();
});

// Global socket instance
let socket;
let currentEventFilter = 'all';
const maxEvents = 100; // Maximum number of events to keep in the log

/**
 * Initialize the Socket.IO connection
 */
function initializeSocket() {
    // Get the connection status indicator
    const connectionStatus = document.getElementById('connection-status');
    
    // Initialize socket connection
    socket = io();
    
    // Connection event handlers
    socket.on('connect', function() {
        console.log('WebSocket connected');
        connectionStatus.classList.remove('bg-danger');
        connectionStatus.classList.add('bg-success');
        connectionStatus.title = 'Connected';
        
        // Subscribe to events
        subscribeToEvents();
    });
    
    socket.on('disconnect', function() {
        console.log('WebSocket disconnected');
        connectionStatus.classList.remove('bg-success');
        connectionStatus.classList.add('bg-danger');
        connectionStatus.title = 'Disconnected';
    });
    
    socket.on('connect_error', function(error) {
        console.error('Connection error:', error);
        connectionStatus.classList.remove('bg-success');
        connectionStatus.classList.add('bg-danger');
        connectionStatus.title = 'Connection Error';
    });
    
    // Handle metrics updates
    socket.on('metrics_update', function(data) {
        console.log('Metrics update received:', data);
        updateMetricsDisplay(data);
    });
    
    // Handle source status updates
    socket.on('source_status_update', function(data) {
        console.log('Source status update received:', data);
        updateSourceStatusDisplay(data);
    });
    
    // Handle pipeline events
    socket.on('pipeline_event', function(event) {
        console.log('Pipeline event received:', event);
        addEventToLog(event);
    });
}

/**
 * Subscribe to events from the server
 */
function subscribeToEvents(sourceFilter = null) {
    socket.emit('subscribe', {
        sources: sourceFilter ? [sourceFilter] : null
    });
}

/**
 * Set up event filter handlers
 */
function setupEventFilters() {
    const filterLinks = document.querySelectorAll('.dropdown-menu [data-filter]');
    
    filterLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            
            // Get the selected filter
            const filter = this.getAttribute('data-filter');
            currentEventFilter = filter;
            
            // Update dropdown button text
            document.getElementById('eventFilterDropdown').textContent = 
                filter === 'all' ? 'All Events' : 
                `${filter.charAt(0).toUpperCase() + filter.slice(1)} Events`;
            
            // Apply the filter to the event log
            applyEventFilter(filter);
        });
    });
}

/**
 * Apply a filter to the event log
 */
function applyEventFilter(filter) {
    const eventItems = document.querySelectorAll('#event-log .event-item');
    
    eventItems.forEach(item => {
        if (filter === 'all' || item.classList.contains(filter)) {
            item.style.display = '';
        } else {
            item.style.display = 'none';
        }
    });
}

/**
 * Update metrics display based on WebSocket data
 */
function updateMetricsDisplay(metrics) {
    // Update the metrics in the UI
    if (!metrics) return;
    
    // Update basic metrics
    if (document.getElementById('sourcesProcessed')) {
        document.getElementById('sourcesProcessed').textContent = metrics.sources_processed || 0;
    }
    
    if (document.getElementById('recordsProcessed')) {
        document.getElementById('recordsProcessed').textContent = formatNumber(metrics.records_processed || 0);
    }
    
    if (document.getElementById('goldRecords')) {
        document.getElementById('goldRecords').textContent = formatNumber(metrics.gold_count || 0);
    }
    
    if (document.getElementById('failedSources')) {
        document.getElementById('failedSources').textContent = metrics.sources_failed || 0;
    }
    
    if (document.getElementById('lastUpdated')) {
        document.getElementById('lastUpdated').textContent = `Last Updated: ${metrics.last_run || 'Never'}`;
    }
    
    // Update classification chart if it exists
    if (window.classificationChart) {
        window.classificationChart.data.datasets[0].data = [
            metrics.gold_count || 0,
            metrics.silver_count || 0,
            metrics.bronze_count || 0,
            metrics.rejected_count || 0
        ];
        window.classificationChart.update();
    }
}

/**
 * Update source status display based on WebSocket data
 */
function updateSourceStatusDisplay(source) {
    // Find an existing row for this source or add a new one to the activity table
    if (!source || !source.name) return;
    
    const activityTableBody = document.querySelector('#activityTable tbody');
    if (!activityTableBody) return;
    
    // Look for existing row
    let sourceRow = Array.from(activityTableBody.querySelectorAll('tr')).find(
        row => row.querySelector('td:nth-child(2)') && 
               row.querySelector('td:nth-child(2)').textContent === source.name
    );
    
    // Generate status badge
    let statusBadge = '';
    if (source.status === 'success') {
        statusBadge = '<span class="badge bg-success">Success</span>';
    } else if (source.status === 'failed') {
        statusBadge = '<span class="badge bg-danger">Failed</span>';
    } else {
        statusBadge = '<span class="badge bg-secondary">Pending</span>';
    }
    
    // If no existing row, add a new one
    if (!sourceRow && source.last_run !== 'Never') {
        // Remove "No recent activity" row if it exists
        const noActivityRow = activityTableBody.querySelector('tr td[colspan="5"]');
        if (noActivityRow) {
            noActivityRow.parentNode.remove();
        }
        
        const newRow = document.createElement('tr');
        newRow.className = 'new-row';
        newRow.innerHTML = `
            <td>${source.last_run || '—'}</td>
            <td>${source.name}</td>
            <td>Data Ingestion</td>
            <td>${statusBadge}</td>
            <td>${formatNumber(source.record_count || 0)}</td>
        `;
        
        // Add highlight animation
        newRow.style.animation = 'fadeIn 0.5s ease-in-out';
        
        // Add to top of table
        if (activityTableBody.firstChild) {
            activityTableBody.insertBefore(newRow, activityTableBody.firstChild);
        } else {
            activityTableBody.appendChild(newRow);
        }
        
        // Limit number of rows
        const maxRows = 10;
        const rows = activityTableBody.querySelectorAll('tr');
        if (rows.length > maxRows) {
            for (let i = maxRows; i < rows.length; i++) {
                rows[i].remove();
            }
        }
    } 
    // Update existing row
    else if (sourceRow) {
        sourceRow.cells[0].textContent = source.last_run || '—';
        sourceRow.cells[3].innerHTML = statusBadge;
        sourceRow.cells[4].textContent = formatNumber(source.record_count || 0);
        
        // Add highlight flash
        sourceRow.style.animation = 'none';
        setTimeout(() => {
            sourceRow.style.animation = 'fadeIn 0.5s ease-in-out';
        }, 10);
    }
}

/**
 * Add an event to the event log
 */
function addEventToLog(event) {
    if (!event) return;
    
    const eventLog = document.getElementById('event-log');
    if (!eventLog) return;
    
    // Remove the placeholder if it exists
    const placeholder = eventLog.querySelector('.text-center.text-muted');
    if (placeholder) {
        placeholder.remove();
    }
    
    // Create event item
    const eventItem = document.createElement('div');
    eventItem.className = `event-item ${event.event_type || 'unknown'} new`;
    
    // Determine icon based on event type
    let icon = 'activity';
    if (event.event_type === 'metrics') {
        icon = 'bar-chart-2';
    } else if (event.event_type === 'source_status') {
        icon = 'database';
    } else if (event.event_type === 'schema_event') {
        icon = 'file-text';
    } else if (event.event_type === 'error') {
        icon = 'alert-triangle';
    }
    
    // Format message based on event type
    let message = event.message || 'Event received';
    
    if (event.event_type === 'metrics' && event.metrics) {
        const metrics = event.metrics;
        message = `Processed ${formatNumber(metrics.records_processed || 0)} records`;
        if (metrics.gold_count) {
            message += ` (${formatNumber(metrics.gold_count)} gold)`;
        }
    } else if (event.event_type === 'source_status' && event.status) {
        const status = event.status;
        message = `Source ${status.status === 'success' ? 'successfully processed' : 'failed processing'}`;
        if (status.record_count) {
            message += ` ${formatNumber(status.record_count)} records`;
        }
    } else if (event.event_type === 'schema_event' && event.data) {
        const schemaEvent = event.data;
        const schemaEventType = schemaEvent.event_type;
        
        if (schemaEventType === 'schema_updated') {
            message = `Schema '${schemaEvent.schema_name}' updated`;
        } else if (schemaEventType === 'schema_evolved') {
            message = `Schema '${schemaEvent.schema_name}' evolved to version ${schemaEvent.version}`;
        } else if (schemaEventType === 'evolution_requested') {
            message = `Evolution requested for schema '${schemaEvent.schema_name}'`;
        }
    }
    
    // Set event HTML
    eventItem.innerHTML = `
        <div class="event-timestamp">${event.timestamp || new Date().toISOString()}</div>
        <div class="event-source">
            <i data-feather="${icon}" class="feather-small"></i>
            ${event.source_name || 'System'}
        </div>
        <p class="event-message">${message}</p>
    `;
    
    // Add event details if available
    if (event.details || (event.event_type === 'metrics' && event.metrics) || 
        (event.event_type === 'schema_event' && event.data)) {
        
        let details = event.details;
        
        if (!details) {
            if (event.event_type === 'metrics' && event.metrics) {
                details = event.metrics;
            } else if (event.event_type === 'schema_event' && event.data) {
                details = event.data;
            }
        }
        
        if (details) {
            const detailsElement = document.createElement('div');
            detailsElement.className = 'event-details';
            
            if (typeof details === 'object') {
                detailsElement.textContent = JSON.stringify(details, null, 2);
            } else {
                detailsElement.textContent = details;
            }
            
            eventItem.appendChild(detailsElement);
        }
    }
    
    // Add event to the top of the log
    if (eventLog.firstChild) {
        eventLog.insertBefore(eventItem, eventLog.firstChild);
    } else {
        eventLog.appendChild(eventItem);
    }
    
    // Apply filter if needed
    if (currentEventFilter !== 'all' && !eventItem.classList.contains(currentEventFilter)) {
        eventItem.style.display = 'none';
    }
    
    // Initialize Feather icons
    if (window.feather) {
        window.feather.replace({
            'stroke-width': 2,
            'width': 16,
            'height': 16
        });
    }
    
    // Limit the number of events
    const eventItems = eventLog.querySelectorAll('.event-item');
    if (eventItems.length > maxEvents) {
        for (let i = maxEvents; i < eventItems.length; i++) {
            eventItems[i].remove();
        }
    }
    
    // Remove 'new' class after animation
    setTimeout(() => {
        eventItem.classList.remove('new');
    }, 1000);
}

/**
 * Format a number with thousands separator
 */
function formatNumber(num) {
    return num.toLocaleString();
}