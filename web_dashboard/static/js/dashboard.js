/**
 * Dashboard JavaScript for the Data Ingestion Pipeline
 * Provides common functionality across dashboard pages
 */

// Format a number with thousand separators
function formatNumber(number) {
    return number.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Format a date as a human-readable string
function formatDate(dateString) {
    if (!dateString || dateString === 'Never') return 'Never';
    
    const date = new Date(dateString);
    return date.toLocaleString();
}

// Calculate time elapsed since a timestamp
function timeElapsed(timestamp) {
    if (!timestamp) return '';
    
    const now = new Date();
    const then = new Date(timestamp);
    const diffMs = now - then;
    
    // Convert to seconds
    const diffSecs = Math.floor(diffMs / 1000);
    
    if (diffSecs < 60) {
        return `${diffSecs} seconds ago`;
    }
    
    // Convert to minutes
    const diffMins = Math.floor(diffSecs / 60);
    
    if (diffMins < 60) {
        return `${diffMins} minute${diffMins !== 1 ? 's' : ''} ago`;
    }
    
    // Convert to hours
    const diffHours = Math.floor(diffMins / 60);
    
    if (diffHours < 24) {
        return `${diffHours} hour${diffHours !== 1 ? 's' : ''} ago`;
    }
    
    // Convert to days
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays} day${diffDays !== 1 ? 's' : ''} ago`;
}

// Get color for quality metric value
function getQualityColor(value) {
    if (value >= 0.9) {
        return '#28a745'; // Green for good
    } else if (value >= 0.8) {
        return '#ffc107'; // Yellow for medium
    } else if (value >= 0.7) {
        return '#fd7e14'; // Orange for borderline
    } else {
        return '#dc3545'; // Red for poor
    }
}

// Create a quality badge element with appropriate color
function createQualityBadge(value, label = '') {
    if (value === undefined || value === null) {
        return '<span class="badge bg-secondary">N/A</span>';
    }
    
    const color = getQualityColor(value);
    const percentage = (value * 100).toFixed(1);
    
    let badgeClass = 'bg-success';
    if (color === '#ffc107') {
        badgeClass = 'bg-warning text-dark';
    } else if (color === '#fd7e14') {
        badgeClass = 'bg-orange text-white';
    } else if (color === '#dc3545') {
        badgeClass = 'bg-danger';
    }
    
    return `<span class="badge ${badgeClass}">${label} ${percentage}%</span>`;
}

// Create classification badge
function createClassificationBadge(classification) {
    if (!classification || classification === 'N/A') {
        return '<span class="badge bg-secondary">N/A</span>';
    }
    
    switch (classification.toLowerCase()) {
        case 'gold':
            return '<span class="badge bg-warning text-dark">Gold</span>';
        case 'silver':
            return '<span class="badge bg-light text-dark">Silver</span>';
        case 'bronze':
            return '<span class="badge bg-secondary">Bronze</span>';
        case 'rejected':
            return '<span class="badge bg-danger">Rejected</span>';
        default:
            return `<span class="badge bg-secondary">${classification}</span>`;
    }
}

// Create status badge
function createStatusBadge(status) {
    if (!status) {
        return '<span class="badge bg-secondary">Unknown</span>';
    }
    
    switch (status.toLowerCase()) {
        case 'success':
            return '<span class="badge bg-success">Success</span>';
        case 'failed':
            return '<span class="badge bg-danger">Failed</span>';
        case 'pending':
            return '<span class="badge bg-secondary">Pending</span>';
        case 'processing':
            return '<span class="badge bg-primary">Processing</span>';
        default:
            return `<span class="badge bg-secondary">${status}</span>`;
    }
}

// Refresh dashboard data
function refreshDashboardData() {
    // Fetch updated metrics
    fetch('/api/metrics')
        .then(response => response.json())
        .then(data => {
            // Update metrics on the page
            const elements = {
                'sources-processed': data.sources_processed,
                'records-processed': formatNumber(data.records_processed),
                'gold-count': formatNumber(data.gold_count),
                'silver-count': formatNumber(data.silver_count),
                'bronze-count': formatNumber(data.bronze_count),
                'rejected-count': formatNumber(data.rejected_count),
                'sources-failed': data.sources_failed,
                'last-run': data.last_run || 'Never'
            };
            
            for (const [id, value] of Object.entries(elements)) {
                const element = document.getElementById(id);
                if (element) {
                    element.textContent = value;
                }
            }
            
            // Trigger event for chart updates
            const event = new CustomEvent('dashboard-data-updated', { detail: data });
            document.dispatchEvent(event);
        })
        .catch(error => {
            console.error('Error fetching dashboard data:', error);
        });
}

// Copy text to clipboard
function copyToClipboard(text) {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand('copy');
    document.body.removeChild(textarea);
    
    // Show toast notification
    showToast('Text copied to clipboard');
}

// Show toast notification
function showToast(message, type = 'info') {
    // Create toast container if it doesn't exist
    let toastContainer = document.querySelector('.toast-container');
    
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.className = 'toast-container position-fixed bottom-0 end-0 p-3';
        document.body.appendChild(toastContainer);
    }
    
    // Create toast element
    const toastId = `toast-${Date.now()}`;
    const toast = document.createElement('div');
    toast.className = `toast align-items-center text-white bg-${type}`;
    toast.id = toastId;
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'assertive');
    toast.setAttribute('aria-atomic', 'true');
    
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                ${message}
            </div>
            <button type="button" class="btn-close me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
        </div>
    `;
    
    toastContainer.appendChild(toast);
    
    // Initialize and show the toast
    const toastInstance = new bootstrap.Toast(toast, { delay: 3000 });
    toastInstance.show();
    
    // Remove toast after it's hidden
    toast.addEventListener('hidden.bs.toast', function() {
        toast.remove();
    });
}

// Initialize common dashboard behaviors when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Initialize feather icons
    if (typeof feather !== 'undefined') {
        feather.replace();
    }
    
    // Add animation when refresh buttons are clicked
    document.querySelectorAll('.refresh-btn').forEach(button => {
        button.addEventListener('click', function() {
            this.classList.add('spin');
            setTimeout(() => {
                this.classList.remove('spin');
            }, 1000);
        });
    });
    
    // Make code blocks copyable
    document.querySelectorAll('pre code').forEach(block => {
        const copyButton = document.createElement('button');
        copyButton.className = 'copy-button btn btn-sm btn-secondary';
        copyButton.textContent = 'Copy';
        copyButton.addEventListener('click', function() {
            copyToClipboard(block.textContent);
            this.textContent = 'Copied!';
            setTimeout(() => {
                this.textContent = 'Copy';
            }, 2000);
        });
        
        const wrapper = document.createElement('div');
        wrapper.className = 'code-wrapper position-relative';
        block.parentNode.insertBefore(wrapper, block);
        wrapper.appendChild(block);
        wrapper.appendChild(copyButton);
    });
});
