/**
 * Real-time streaming functions for data flow dashboard
 * Uses Socket.IO to establish WebSocket connections
 */

// Store socket connection
let socket = null;

// Define custom event handlers
const eventHandlers = {
    'pipeline_metrics': [],
    'source_status': [],
    'event': [],
    'events': []
};

// Initialize the WebSocket connection
function initializeSocket() {
    // Check if Socket.IO is loaded
    if (typeof io === 'undefined') {
        console.error('Socket.IO library not loaded');
        return false;
    }

    // Create socket connection
    socket = io();

    // Set up event handlers
    socket.on('connect', () => {
        console.log('Connected to server');
        // Dispatch custom event
        document.dispatchEvent(new CustomEvent('socket:connected'));
    });

    socket.on('disconnect', () => {
        console.log('Disconnected from server');
        // Dispatch custom event
        document.dispatchEvent(new CustomEvent('socket:disconnected'));
    });

    socket.on('pipeline_metrics', (data) => {
        console.log('Received pipeline metrics', data);
        // Call event handlers
        eventHandlers.pipeline_metrics.forEach(handler => handler(data));
        // Dispatch custom event
        document.dispatchEvent(new CustomEvent('socket:pipeline_metrics', { detail: data }));
    });

    socket.on('source_status', (data) => {
        console.log('Received source status', data);
        // Call event handlers
        eventHandlers.source_status.forEach(handler => handler(data));
        // Dispatch custom event
        document.dispatchEvent(new CustomEvent('socket:source_status', { detail: data }));
    });

    socket.on('event', (data) => {
        console.log('Received event', data);
        // Call event handlers
        eventHandlers.event.forEach(handler => handler(data));
        // Dispatch custom event
        document.dispatchEvent(new CustomEvent('socket:event', { detail: data }));
    });

    socket.on('events', (data) => {
        console.log('Received events', data);
        // Call event handlers
        eventHandlers.events.forEach(handler => handler(data));
        // Dispatch custom event
        document.dispatchEvent(new CustomEvent('socket:events', { detail: data }));
    });

    return true;
}

// Register event handler
function onSocketEvent(eventName, handler) {
    if (eventHandlers[eventName]) {
        eventHandlers[eventName].push(handler);
    } else {
        console.warn(`Unknown event: ${eventName}`);
    }
}

// Subscribe to events for a specific source
function subscribeToEvents(sourceFilter = null) {
    if (!socket) {
        console.error('Socket not initialized');
        return false;
    }

    socket.emit('subscribe_to_events', { source_filter: sourceFilter });
    return true;
}

// Update metrics display
function updateMetricsDisplay(metrics) {
    // Update counts
    if (document.getElementById('sources-processed')) {
        document.getElementById('sources-processed').textContent = metrics.sources_processed.toLocaleString();
    }
    
    if (document.getElementById('records-processed')) {
        document.getElementById('records-processed').textContent = metrics.records_processed.toLocaleString();
    }
    
    if (document.getElementById('bronze-count')) {
        document.getElementById('bronze-count').textContent = metrics.bronze_count.toLocaleString();
    }
    
    if (document.getElementById('silver-count')) {
        document.getElementById('silver-count').textContent = metrics.silver_count.toLocaleString();
    }
    
    if (document.getElementById('gold-count')) {
        document.getElementById('gold-count').textContent = metrics.gold_count.toLocaleString();
    }
    
    if (document.getElementById('rejected-count')) {
        document.getElementById('rejected-count').textContent = metrics.rejected_count.toLocaleString();
    }
    
    if (document.getElementById('last-run')) {
        document.getElementById('last-run').textContent = metrics.last_run || 'Never';
    }

    // Update history chart if available
    if (typeof updateMetricsChart === 'function' && metrics.history) {
        updateMetricsChart(metrics.history);
    }
}

// Update source status display
function updateSourceStatusDisplay(sources) {
    const tableBody = document.getElementById('source-status-table-body');
    if (!tableBody) return;

    // Clear existing rows
    tableBody.innerHTML = '';

    // Add rows for each source
    sources.forEach(source => {
        const row = document.createElement('tr');
        
        // Add status class
        if (source.status === 'success') {
            row.classList.add('table-success');
        } else if (source.status === 'error') {
            row.classList.add('table-danger');
        } else if (source.status === 'pending') {
            row.classList.add('table-warning');
        }
        
        row.innerHTML = `
            <td>${source.name}</td>
            <td>${source.type}</td>
            <td>${source.last_run}</td>
            <td><span class="badge bg-${
                source.status === 'success' ? 'success' : 
                source.status === 'error' ? 'danger' : 
                'warning'
            }">${source.status}</span></td>
            <td>${source.classification}</td>
            <td>${source.record_count.toLocaleString()}</td>
        `;
        
        tableBody.appendChild(row);
    });
}

// Add an event to the event log display
function addEventToLog(event) {
    const eventLog = document.getElementById('event-log');
    if (!eventLog) return;

    // Create event row
    const eventRow = document.createElement('div');
    eventRow.classList.add('event-row', 'card', 'mb-2');
    
    // Add event type class
    if (event.event_type === 'metrics') {
        eventRow.classList.add('border-info');
    } else if (event.event_type === 'source_status') {
        eventRow.classList.add('border-primary');
    } else if (event.event_type === 'error') {
        eventRow.classList.add('border-danger');
    } else {
        eventRow.classList.add('border-secondary');
    }
    
    // Event content
    eventRow.innerHTML = `
        <div class="card-body">
            <div class="d-flex justify-content-between">
                <h6 class="card-subtitle mb-2 text-muted">[${event.timestamp}] ${event.source_name}</h6>
                <span class="badge bg-${
                    event.event_type === 'metrics' ? 'info' : 
                    event.event_type === 'source_status' ? 'primary' : 
                    event.event_type === 'error' ? 'danger' : 
                    'secondary'
                }">${event.event_type}</span>
            </div>
            <p class="card-text">${JSON.stringify(event.data, null, 2)}</p>
        </div>
    `;
    
    // Add to log (at the beginning)
    eventLog.insertBefore(eventRow, eventLog.firstChild);
    
    // Limit number of events
    if (eventLog.children.length > 50) {
        eventLog.removeChild(eventLog.lastChild);
    }
}

// Initialize the real-time functionality
document.addEventListener('DOMContentLoaded', () => {
    // Initialize socket
    const socketInitialized = initializeSocket();
    if (!socketInitialized) {
        console.warn('Failed to initialize WebSocket connection');
    }
    
    // Register event handlers
    onSocketEvent('pipeline_metrics', updateMetricsDisplay);
    onSocketEvent('source_status', updateSourceStatusDisplay);
    onSocketEvent('event', addEventToLog);
    
    // Subscribe to events
    if (socketInitialized) {
        subscribeToEvents();
    }
    
    // Set up connection status indicator
    const statusIndicator = document.getElementById('connection-status');
    if (statusIndicator) {
        document.addEventListener('socket:connected', () => {
            statusIndicator.classList.remove('bg-danger');
            statusIndicator.classList.add('bg-success');
            statusIndicator.title = 'Connected';
        });
        
        document.addEventListener('socket:disconnected', () => {
            statusIndicator.classList.remove('bg-success');
            statusIndicator.classList.add('bg-danger');
            statusIndicator.title = 'Disconnected';
        });
    }
});