/**
 * Interactive Data Flow Animation for the Data Ingestion Pipeline
 * Provides animated visualizations of data flowing through the pipeline
 */

// Configuration for the animation system
const dataFlowConfig = {
    animationDuration: 1500,      // Duration of a single animation in ms
    particleCount: 15,            // Number of particles to generate per flow
    particleSize: 4,              // Size of particles in pixels
    particleVariation: 0.5,       // Variation in particle size (0-1)
    pathVariation: 8,             // Variation in particle path (pixels)
    animationDelay: 3000,         // Delay between animations in ms
    pathOpacity: 0.5,             // Opacity of the flow path
    autoAnimate: false,           // Automatically start animations
    showFlowRate: true,           // Show flow rate indicators
    showTooltips: true,           // Show tooltips on hover
    enableInteractions: true,     // Enable user interactions
    randomFlows: true             // Randomly select flows for animation
};

// Store paths for animations
let flowPaths = {};
let animationActive = false;
let animationInterval = null;

/**
 * Initialize the data flow animation system
 */
function initDataFlowAnimation() {
    console.log('Initializing data flow animation...');
    
    // Find the data flow diagram SVG
    const diagram = document.getElementById('dataFlowDiagram');
    if (!diagram) {
        console.warn('Data flow diagram not found');
        return;
    }
    
    // Wait for the MermaidJS diagram to be fully rendered
    const observer = new MutationObserver((mutations, obs) => {
        const svg = diagram.querySelector('svg');
        if (svg) {
            // Diagram is rendered, initialize animations
            obs.disconnect();
            console.log('Data flow diagram found, initializing animations');
            
            // Initialize paths after a short delay to ensure complete rendering
            setTimeout(() => {
                extractFlowPaths(svg);
                setupUserInteractions(svg);
                if (dataFlowConfig.autoAnimate) {
                    startFlowAnimations();
                }
            }, 500);
        }
    });
    
    observer.observe(diagram, {
        childList: true,
        subtree: true
    });
}

/**
 * Extract flow paths from the diagram SVG
 */
function extractFlowPaths(svg) {
    // Get all path elements representing flows
    const pathElements = svg.querySelectorAll('path.flowchart-link');
    const nodeElements = svg.querySelectorAll('.node');
    
    // Create a mapping of node IDs to node elements
    const nodeMap = {};
    nodeElements.forEach(node => {
        const id = node.id;
        if (id) {
            nodeMap[id] = node;
        }
    });
    
    // Reset flows
    flowPaths = {};
    
    // Extract path data and metadata
    pathElements.forEach((pathElement, index) => {
        // Get associated nodes by extracting IDs from marker references
        const pathId = pathElement.id || `path-${index}`;
        const sourceNodeId = extractNodeIdFromPath(pathElement, 'source');
        const targetNodeId = extractNodeIdFromPath(pathElement, 'target');
        
        if (sourceNodeId && targetNodeId) {
            // Skip paths where both nodes aren't found
            if (!nodeMap[sourceNodeId] || !nodeMap[targetNodeId]) {
                return;
            }
            
            // Create a flow ID
            const flowId = `${sourceNodeId}-to-${targetNodeId}`;
            
            // Extract node labels for better identification
            const sourceLabel = extractNodeLabel(nodeMap[sourceNodeId]);
            const targetLabel = extractNodeLabel(nodeMap[targetNodeId]);
            
            // Store the path data
            flowPaths[flowId] = {
                id: flowId,
                pathElement: pathElement,
                pathData: pathElement.getAttribute('d'),
                sourceNodeId: sourceNodeId,
                targetNodeId: targetNodeId,
                sourceNode: nodeMap[sourceNodeId],
                targetNode: nodeMap[targetNodeId],
                sourceLabel: sourceLabel,
                targetLabel: targetLabel,
                // Determine flow type based on node classes and relationships
                flowType: determineFlowType(nodeMap[sourceNodeId], nodeMap[targetNodeId])
            };
        }
    });
    
    console.log(`Extracted ${Object.keys(flowPaths).length} flow paths`);
}

/**
 * Extract a node ID from a path element
 */
function extractNodeIdFromPath(pathElement, type) {
    // Extract node ID from marker reference or path data
    try {
        const markerRef = type === 'source' ? 
            pathElement.getAttribute('marker-start') : 
            pathElement.getAttribute('marker-end');
        
        if (markerRef) {
            // Extract ID from marker reference
            const match = markerRef.match(/url\(#([^)]+)\)/);
            if (match && match[1]) {
                const markerId = match[1];
                
                // Try to find the node ID from the marker ID
                if (markerId.startsWith(`${type}Mark-`)) {
                    return markerId.replace(`${type}Mark-`, '');
                }
            }
        }
        
        // Fallback to data analysis
        const d = pathElement.getAttribute('d');
        if (d) {
            // Parse path data for endpoints
            const pointsMatch = d.match(/M\s*([0-9.-]+)\s*,?\s*([0-9.-]+)/);
            if (pointsMatch) {
                // Return placeholder ID based on coordinates
                return type === 'source' ? 
                    `node-${Math.round(parseFloat(pointsMatch[1]))}-${Math.round(parseFloat(pointsMatch[2]))}` : 
                    `node-end-${d.length}`;
            }
        }
    } catch (e) {
        console.warn('Error extracting node ID:', e);
    }
    
    return null;
}

/**
 * Extract node label from a node element
 */
function extractNodeLabel(nodeElement) {
    // Try to get the text content of the node
    const textElement = nodeElement.querySelector('text');
    if (textElement) {
        return textElement.textContent.trim();
    }
    
    // Fallback to node ID
    return nodeElement.id || 'Unknown Node';
}

/**
 * Determine the type of flow based on source and target nodes
 */
function determineFlowType(sourceNode, targetNode) {
    // Check node classes to determine the type of flow
    const sourceClasses = sourceNode.getAttribute('class') || '';
    const targetClasses = targetNode.getAttribute('class') || '';
    
    if (sourceClasses.includes('source') || 
        sourceNode.id.includes('DS_') || 
        sourceNode.id.includes('source')) {
        return 'data-source';
    } else if (targetClasses.includes('elastic') || 
               targetNode.id.includes('ES_') || 
               targetNode.id.includes('elastic')) {
        return 'elasticsearch';
    } else if (sourceClasses.includes('classification') || 
               sourceNode.id.includes('BZ') || 
               sourceNode.id.includes('SV') || 
               sourceNode.id.includes('GD') || 
               sourceNode.id.includes('RJ')) {
        return 'classification';
    } else if (targetClasses.includes('hive') || 
               targetNode.id.includes('HV_') || 
               targetNode.id.includes('hive')) {
        return 'hive';
    } else if (sourceClasses.includes('processing') || 
               targetClasses.includes('processing')) {
        return 'processing';
    }
    
    return 'default';
}

/**
 * Create and animate particles along a path
 */
function animateFlow(flowId) {
    const flow = flowPaths[flowId];
    if (!flow) return;
    
    // Get the SVG container
    const svg = flow.pathElement.closest('svg');
    if (!svg) return;
    
    // Create a group for particles
    const particleGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    particleGroup.classList.add('data-particle-group');
    particleGroup.setAttribute('data-flow', flowId);
    svg.appendChild(particleGroup);
    
    // Create particles
    for (let i = 0; i < dataFlowConfig.particleCount; i++) {
        // Create particle with random properties
        setTimeout(() => {
            createParticle(flow, particleGroup, i);
        }, (i / dataFlowConfig.particleCount) * dataFlowConfig.animationDuration);
    }
    
    // Clean up after animation
    setTimeout(() => {
        particleGroup.remove();
    }, dataFlowConfig.animationDuration + 500);
}

/**
 * Create a single animated particle
 */
function createParticle(flow, group, index) {
    // Create the particle element
    const particle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    
    // Set initial properties
    const particleSize = dataFlowConfig.particleSize * 
        (1 - Math.random() * dataFlowConfig.particleVariation);
    
    particle.setAttribute('r', particleSize);
    particle.classList.add('data-particle');
    
    // Set particle color based on flow type
    const particleClass = getFlowTypeClass(flow.flowType);
    particle.classList.add(particleClass);
    
    // Add particle to group
    group.appendChild(particle);
    
    // Animate the particle along the path
    animateParticle(particle, flow, index);
}

/**
 * Animate a particle along a path
 */
function animateParticle(particle, flow, index) {
    // Create a path element for animation
    const pathElement = flow.pathElement;
    const pathLength = pathElement.getTotalLength();
    
    // Randomize starting point slightly for natural flow
    const startMultiplier = index / dataFlowConfig.particleCount;
    let startPosition = startMultiplier * 0.2;
    
    // Create animation variables
    const startTime = performance.now();
    const duration = dataFlowConfig.animationDuration * (0.8 + Math.random() * 0.4);
    
    // Start animation loop
    requestAnimationFrame(animate);
    
    function animate(timestamp) {
        // Calculate progress along path
        const elapsed = timestamp - startTime;
        let progress = Math.min(elapsed / duration, 1);
        
        // Add particle starting offset
        progress = Math.min(startPosition + progress * (1 - startPosition), 1);
        
        // Get position along path
        const point = pathElement.getPointAtLength(progress * pathLength);
        
        // Add small random variation to position for natural flow
        const variation = dataFlowConfig.pathVariation;
        const x = point.x + (Math.random() - 0.5) * variation;
        const y = point.y + (Math.random() - 0.5) * variation;
        
        // Update particle position
        particle.setAttribute('cx', x);
        particle.setAttribute('cy', y);
        
        // Continue animation if not complete
        if (progress < 1) {
            requestAnimationFrame(animate);
        } else {
            // Remove particle when animation completes
            setTimeout(() => {
                particle.remove();
            }, 100);
        }
    }
}

/**
 * Start flow animations
 */
function startFlowAnimations() {
    if (animationActive) return;
    
    console.log('Starting data flow animations');
    animationActive = true;
    
    // Highlight paths to indicate active flows
    Object.keys(flowPaths).forEach(flowId => {
        const flow = flowPaths[flowId];
        flow.pathElement.classList.add('active-flow');
    });
    
    // Schedule animations
    animateNextFlow();
    
    // Set interval for continuous animation
    animationInterval = setInterval(animateNextFlow, dataFlowConfig.animationDelay);
}

/**
 * Animate a randomly selected flow
 */
function animateNextFlow() {
    // Get all flow IDs
    const flowIds = Object.keys(flowPaths);
    if (flowIds.length === 0) return;
    
    // Select a random flow or follow a logical sequence
    // For now, we'll just select a random flow
    const randomIndex = Math.floor(Math.random() * flowIds.length);
    const selectedFlowId = flowIds[randomIndex];
    
    // Animate the selected flow
    animateFlow(selectedFlowId);
    
    // Also check for flows that should happen together
    // For example, if a classification flow is selected, also animate the storage flows
    const selectedFlow = flowPaths[selectedFlowId];
    if (selectedFlow && selectedFlow.flowType === 'classification') {
        // Find all storage flows from the same source
        Object.keys(flowPaths).forEach(flowId => {
            const flow = flowPaths[flowId];
            if ((flow.flowType === 'hive' || flow.flowType === 'elasticsearch') && 
                flow.sourceNodeId === selectedFlow.sourceNodeId) {
                setTimeout(() => {
                    animateFlow(flowId);
                }, 300);
            }
        });
    }
}

/**
 * Stop flow animations
 */
function stopFlowAnimations() {
    if (!animationActive) return;
    
    console.log('Stopping data flow animations');
    animationActive = false;
    
    // Clear animation interval
    if (animationInterval) {
        clearInterval(animationInterval);
        animationInterval = null;
    }
    
    // Remove all remaining particles
    const particles = document.querySelectorAll('.data-particle, .data-particle-group');
    particles.forEach(particle => {
        particle.remove();
    });
    
    // Remove active flow indicators
    Object.keys(flowPaths).forEach(flowId => {
        const flow = flowPaths[flowId];
        flow.pathElement.classList.remove('active-flow');
    });
}

/**
 * Toggle flow animations
 */
function toggleFlowAnimations() {
    if (animationActive) {
        stopFlowAnimations();
    } else {
        startFlowAnimations();
    }
    
    return animationActive;
}

/**
 * Set up user interactions with the diagram
 */
function setupUserInteractions(svg) {
    if (!dataFlowConfig.enableInteractions) return;
    
    // Find existing controls that we added to the HTML
    const toggleButton = document.getElementById('toggle-animation');
    const speedSlider = document.getElementById('animation-speed');
    const speedValue = document.getElementById('speed-value');
    const randomFlow = document.getElementById('random-flow');
    const highlightHive = document.getElementById('highlight-hive');
    const highlightES = document.getElementById('highlight-elasticsearch');
    const flowTooltip = document.getElementById('flow-tooltip');
    const flowTooltipHeader = document.getElementById('flow-tooltip-header');
    const flowTooltipBody = document.getElementById('flow-tooltip-body');
    
    // Setup toggle button
    if (toggleButton) {
        toggleButton.addEventListener('click', () => {
            const isActive = toggleFlowAnimations();
            toggleButton.innerHTML = isActive ? 
                '<i class="bi bi-pause-fill"></i> Pause Animation' : 
                '<i class="bi bi-play-fill"></i> Start Animation';
        });
    }
    
    // Setup speed slider
    if (speedSlider && speedValue) {
        speedSlider.addEventListener('input', (e) => {
            const speedFactor = parseFloat(e.target.value);
            speedValue.textContent = speedFactor + 'x';
            dataFlowConfig.animationDuration = 1500 / speedFactor;
            dataFlowConfig.animationDelay = 3000 / speedFactor;
            
            // Restart animation with new settings if active
            if (animationActive) {
                stopFlowAnimations();
                startFlowAnimations();
            }
        });
    }
    
    // Setup random flow checkbox
    if (randomFlow) {
        randomFlow.addEventListener('change', (e) => {
            dataFlowConfig.randomFlows = e.target.checked;
        });
        // Initialize from checkbox state
        dataFlowConfig.randomFlows = randomFlow.checked;
    }
    
    // Setup highlight buttons for specific flow types
    if (highlightHive) {
        highlightHive.addEventListener('click', () => {
            highlightFlowsByType('hive');
        });
    }
    
    if (highlightES) {
        highlightES.addEventListener('click', () => {
            highlightFlowsByType('elasticsearch');
        });
    }
    
    // Setup individual flow interactions
    if (dataFlowConfig.showTooltips) {
        Object.keys(flowPaths).forEach(flowId => {
            const flow = flowPaths[flowId];
            
            // Add hover effects to path
            flow.pathElement.addEventListener('mouseenter', () => {
                highlightFlow(flow);
            });
            
            flow.pathElement.addEventListener('mouseleave', () => {
                unhighlightFlow(flow);
            });
            
            // Add click to trigger specific flow animation
            flow.pathElement.addEventListener('click', (e) => {
                e.stopPropagation();
                animateFlow(flowId);
            });
        });
    }
}

/**
 * Highlight a specific flow
 */
function highlightFlow(flow) {
    flow.pathElement.classList.add('highlighted-flow');
    
    // Highlight connected nodes
    if (flow.sourceNode) {
        flow.sourceNode.classList.add('highlighted-node');
    }
    
    if (flow.targetNode) {
        flow.targetNode.classList.add('highlighted-node');
    }
    
    // Show tooltip with flow information
    showFlowTooltip(flow);
}

/**
 * Remove highlight from a flow
 */
function unhighlightFlow(flow) {
    flow.pathElement.classList.remove('highlighted-flow');
    
    // Remove highlight from nodes
    if (flow.sourceNode) {
        flow.sourceNode.classList.remove('highlighted-node');
    }
    
    if (flow.targetNode) {
        flow.targetNode.classList.remove('highlighted-node');
    }
    
    // Hide tooltip
    hideFlowTooltip();
}

/**
 * Show a tooltip with flow information
 */
function showFlowTooltip(flow) {
    // Remove any existing tooltip
    hideFlowTooltip();
    
    // Create tooltip element
    const tooltip = document.createElement('div');
    tooltip.id = 'flowTooltip';
    tooltip.className = 'flow-tooltip';
    
    // Set tooltip content
    tooltip.innerHTML = `
        <div class="flow-tooltip-header">
            <span class="flow-type ${getFlowTypeClass(flow.flowType)}"></span>
            <strong>${flow.sourceLabel}</strong> → <strong>${flow.targetLabel}</strong>
        </div>
        <div class="flow-tooltip-body">
            <div>Type: <span class="flow-type-label">${formatFlowType(flow.flowType)}</span></div>
        </div>
    `;
    
    // Position the tooltip near the path
    const svgRect = flow.pathElement.ownerSVGElement.getBoundingClientRect();
    const pathRect = flow.pathElement.getBoundingClientRect();
    
    const centerX = pathRect.left + pathRect.width / 2;
    const centerY = pathRect.top + pathRect.height / 2;
    
    tooltip.style.left = `${centerX}px`;
    tooltip.style.top = `${centerY - 30}px`;
    
    // Add to page
    document.body.appendChild(tooltip);
    
    // Ensure tooltip is within viewport
    const tooltipRect = tooltip.getBoundingClientRect();
    
    if (tooltipRect.right > window.innerWidth) {
        tooltip.style.left = `${window.innerWidth - tooltipRect.width - 10}px`;
    }
    
    if (tooltipRect.left < 0) {
        tooltip.style.left = '10px';
    }
}

/**
 * Hide the flow tooltip
 */
function hideFlowTooltip() {
    const tooltip = document.getElementById('flowTooltip');
    if (tooltip) {
        tooltip.remove();
    }
}

/**
 * Highlight flows of a specific type
 */
function highlightFlowsByType(flowType) {
    // First remove all highlights
    Object.keys(flowPaths).forEach(flowId => {
        const flow = flowPaths[flowId];
        unhighlightFlow(flow);
    });
    
    // Then highlight only flows of the specified type
    Object.keys(flowPaths).forEach(flowId => {
        const flow = flowPaths[flowId];
        if (flow.flowType === flowType) {
            highlightFlow(flow);
            
            // Animate this flow
            setTimeout(() => {
                animateFlow(flowId);
            }, 300);
        }
    });
    
    // After a delay, clear highlights
    setTimeout(() => {
        Object.keys(flowPaths).forEach(flowId => {
            const flow = flowPaths[flowId];
            unhighlightFlow(flow);
        });
    }, 5000);
}

/**
 * Format flow type for display
 */
function formatFlowType(flowType) {
    switch (flowType) {
        case 'data-source':
            return 'Data Source';
        case 'elasticsearch':
            return 'Elasticsearch (Secondary)';
        case 'hive':
            return 'Hive (Primary)';
        case 'classification':
            return 'Classification';
        case 'processing':
            return 'Processing';
        default:
            return 'Data Flow';
    }
}

/**
 * Get CSS class for a flow type
 */
function getFlowTypeClass(flowType) {
    switch (flowType) {
        case 'data-source':
            return 'flow-data-source';
        case 'elasticsearch':
            return 'flow-elasticsearch';
        case 'hive':
            return 'flow-hive';
        case 'classification':
            return 'flow-classification';
        case 'processing':
            return 'flow-processing';
        default:
            return 'flow-default';
    }
}

// Initialize animations when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initDataFlowAnimation();
});