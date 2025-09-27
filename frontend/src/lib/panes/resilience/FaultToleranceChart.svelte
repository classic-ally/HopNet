<script lang="ts">
    import { onMount, onDestroy } from 'svelte';
    import uPlot from 'uplot';
    import 'uplot/dist/uPlot.min.css';
    import type { FaultToleranceCurvePoint } from '../../types';
    import { formatStorageCapacity } from '../../utils/formatters';

    export let data: FaultToleranceCurvePoint[] = [];
    export let onPlanClick: (() => void) | undefined = undefined;
    export let planButtonText: string = "Plan...";

    let chartContainer: HTMLDivElement;
    let chart: uPlot | null = null;

    // Chart configuration matching your Catppuccin theme
    const chartOptions: uPlot.Options = {
        width: 600,
        height: 400,
        padding: [20, 20, 20, 10],

        // Theme colors matching your design system
        background: '#313244', // surface0

        scales: {
            x: {
                time: false,
                range: (u, dataMin, dataMax) => {
                    const range = dataMax - dataMin;
                    const padding = range / 20; // 1/20th of the range
                    return [dataMin, dataMax + padding];
                },
            },
            y: {
                range: (u, dataMin, dataMax) => {
                    // Always start from 0
                    const min = 0;

                    // Smart max scaling based on data
                    if (dataMax <= 0) {
                        // All values are 0 - show 0 to 1 for better visualization
                        return [min, 1];
                    } else {
                        // Low values - show with some padding
                        return [min, Math.ceil(dataMax)];
                    }
                }
            }
        },

        axes: [
            {
                // X-axis: User Data (GB) - only show change points
                stroke: '#cdd6f4', // primary text color
                label: 'Raw User Data',
                labelFont: '12px Red Hat Display',
                labelSize: 14,
                grid: {
                    stroke: '#45475a', // surface1 - very subtle
                    width: 0.5,
                    dash: [2, 4], // Light dashed lines
                },
                ticks: {
                    stroke: '#6c7086',
                    width: 1,
                },
                font: '12px Red Hat Display',
                space: 20, // Increased to accommodate axis label
                // Only show X-axis ticks at relevant change points based on visible series
                splits: (u, axisIdx, scaleMin, scaleMax, foundIncr, foundSpace) => {
                    if (data.length === 0) return [];

                    // Check which series are visible
                    const tolerableFailuresVisible = u.series[1]?.show !== false;
                    const activeNodesVisible = u.series[2]?.show !== false;

                    const relevantPoints = [];

                    for (let i = 0; i < data.length; i++) {
                        const point = data[i];
                        let shouldShow = false;

                        if (i === 0 || i === data.length - 1) {
                            // Always show first and last points
                            shouldShow = true;
                        } else {
                            const prevPoint = data[i - 1];

                            // Show if tolerable failures changed and that series is visible
                            if (tolerableFailuresVisible && point.nodes_can_fail !== prevPoint.nodes_can_fail) {
                                shouldShow = true;
                            }

                            // Show if active nodes changed and that series is visible
                            if (activeNodesVisible && point.active_nodes !== prevPoint.active_nodes) {
                                shouldShow = true;
                            }
                        }

                        if (shouldShow) {
                            relevantPoints.push(point.user_data_gb);
                        }
                    }

                    return relevantPoints;
                },
                // Format X-axis values with smart units (GB/TB)
                values: (u, vals) => vals.map(v => formatStorageCapacity(v)),
            },
            {
                // Y-axis: Nodes Can Fail
                stroke: '#cdd6f4', // primary text color
                label: 'Nodes',
                labelFont: '12px Red Hat Display',
                labelSize: 14,
                grid: {
                    stroke: '#45475a', // surface1 - very subtle
                    width: 0.5,
                    dash: [2, 4], // Light dashed lines
                },
                ticks: {
                    stroke: '#6c7086',
                    width: 1,
                },
                font: '12px Red Hat Display',
                space: 25, // Space for axis label
                // Generate integer-only ticks
                splits: (u, axisIdx, scaleMin, scaleMax, foundIncr, foundSpace) => {
                    const min = Math.floor(scaleMin);
                    const max = Math.ceil(scaleMax);
                    const ticks = [];
                    for (let i = min; i <= max; i++) {
                        ticks.push(i);
                    }
                    return ticks;
                },
            }
        ],

        series: [
            {
                // X-axis data (User Data GB)
                label: "User Data (GB)",
            },
            {
                // Tolerable Failures line - step function (custom drawn)
                label: "Tolerable Failures",
                stroke: "transparent", // Hide default stroke, we draw our own
                width: 0,
                fill: (u, seriesIdx) => {
                    try {
                        // Create a gradient with sharp color changes at step points
                        if (!u.bbox || !isFinite(u.bbox.left) || !isFinite(u.bbox.width) || u.bbox.width <= 0) {
                            // Fallback to simple color if bbox is invalid
                            return 'rgba(137, 180, 250, 0.1)';
                        }

                        const plotLeft = u.bbox.left;
                        const plotRight = plotLeft + u.bbox.width;
                        const gradient = u.ctx.createLinearGradient(plotLeft, 0, plotRight, 0);

                        if (!u.data || !u.data[1] || u.data[1].length === 0) {
                            gradient.addColorStop(0, 'rgba(137, 180, 250, 0.1)');
                            return gradient;
                        }

                        const data = u.data[1];
                        const xData = u.data[0];

                        // Convert data coordinates to gradient positions (0-1)
                        for (let i = 0; i < data.length; i++) {
                            if (!isFinite(xData[i]) || !isFinite(data[i])) continue;

                            const xPixel = u.valToPos(xData[i], 'x', true);
                            if (!isFinite(xPixel)) continue;

                            const gradientPos = (xPixel - plotLeft) / u.bbox.width;
                            if (!isFinite(gradientPos)) continue;

                            let color;
                            if (data[i] === 0) {
                                color = 'rgba(243, 139, 168, 0.15)'; // red fill
                            } else if (data[i] === 1) {
                                color = 'rgba(249, 226, 175, 0.15)'; // yellow fill
                            } else {
                                color = 'rgba(137, 180, 250, 0.1)'; // blue fill
                            }

                            const clampedPos = Math.max(0, Math.min(1, gradientPos));
                            gradient.addColorStop(clampedPos, color);

                            // Add the same color just before the next point for sharp transition
                            if (i < data.length - 1 && isFinite(xData[i + 1])) {
                                const nextXPixel = u.valToPos(xData[i + 1], 'x', true);
                                if (isFinite(nextXPixel)) {
                                    const nextGradientPos = (nextXPixel - plotLeft) / u.bbox.width;
                                    if (isFinite(nextGradientPos)) {
                                        const beforeNext = nextGradientPos - 0.001;
                                        if (beforeNext > gradientPos && beforeNext >= 0 && beforeNext <= 1) {
                                            gradient.addColorStop(beforeNext, color);
                                        }
                                    }
                                }
                            }
                        }

                        return gradient;
                    } catch (error) {
                        console.warn('Error creating gradient fill:', error);
                        return 'rgba(137, 180, 250, 0.1)';
                    }
                },
                paths: uPlot.paths.stepped!({ align: 1 }), // Keep stepped paths for fill
                points: {
                    show: true,
                    size: (u, seriesIdx, dataIdx) => {
                        // Make hovered point larger
                        if (u.cursor.idx === dataIdx) return 12;
                        return 6;
                    },
                    stroke: (u, seriesIdx, dataIdx) => {
                        if (!u.data || !u.data[1] || dataIdx >= u.data[1].length) return '#89b4fa';
                        const value = u.data[1][dataIdx];
                        let baseColor;
                        if (value === 0) baseColor = '#f38ba8'; // red
                        else if (value === 1) baseColor = '#f9e2af'; // yellow
                        else baseColor = '#89b4fa'; // blue

                        // Brighter when hovered
                        return u.cursor.idx === dataIdx ? baseColor : baseColor;
                    },
                    fill: (u, seriesIdx, dataIdx) => {
                        if (u.cursor.idx === dataIdx) {
                            // Bright fill when hovered
                            if (!u.data || !u.data[1] || dataIdx >= u.data[1].length) return '#89b4fa';
                            const value = u.data[1][dataIdx];
                            if (value === 0) return '#f38ba8'; // bright red fill
                            if (value === 1) return '#f9e2af'; // bright yellow fill
                            return '#89b4fa'; // bright blue fill
                        }
                        return '#313244'; // surface0 for point centers
                    },
                },
            },
            {
                // Active Nodes line - step function
                label: "Active Nodes",
                stroke: '#a6e3a1', // green for secondary curve
                width: 2,
                dash: [5, 5], // dashed line
                paths: uPlot.paths.stepped!({ align: 1 }), // Right-aligned steps
                points: {
                    show: true,
                    size: (u, seriesIdx, dataIdx) => {
                        // Make hovered point larger
                        if (u.cursor.idx === dataIdx) return 10;
                        return 4;
                    },
                    stroke: '#a6e3a1',
                    fill: (u, seriesIdx, dataIdx) => {
                        if (u.cursor.idx === dataIdx) {
                            // Bright green fill when hovered
                            return '#a6e3a1';
                        }
                        return '#313244'; // surface0 for point centers
                    },
                },
                show: false, // Hidden by default, can be toggled via legend
            }
        ],

        hooks: {
            draw: [
                u => {
                    // Custom drawing for color-coded line segments
                    const ctx = u.ctx;
                    const data = u.data;
                    if (!data || !data[1] || data[1].length === 0) return;

                    // Only draw if the tolerable failures series is visible
                    if (u.series[1].show === false) return;

                    ctx.save();
                    ctx.lineWidth = 3;

                    for (let i = 0; i < data[1].length - 1; i++) {
                        const x1 = u.valToPos(data[0][i], 'x', true);
                        const y1 = u.valToPos(data[1][i], 'y', true);
                        const x2 = u.valToPos(data[0][i + 1], 'x', true);
                        const y2 = y1; // Step function - horizontal line first

                        // Set color based on value
                        if (data[1][i] === 0) {
                            ctx.strokeStyle = '#f38ba8'; // red
                        } else if (data[1][i] === 1) {
                            ctx.strokeStyle = '#f9e2af'; // yellow
                        } else {
                            ctx.strokeStyle = '#89b4fa'; // blue
                        }

                        // Draw horizontal line (step)
                        ctx.beginPath();
                        ctx.moveTo(x1, y1);
                        ctx.lineTo(x2, y2);
                        ctx.stroke();

                        // Draw vertical line to next level
                        if (i < data[1].length - 1) {
                            const y3 = u.valToPos(data[1][i + 1], 'y', true);
                            ctx.beginPath();
                            ctx.moveTo(x2, y2);
                            ctx.lineTo(x2, y3);
                            ctx.stroke();
                        }
                    }

                    ctx.restore();
                }
            ]
        },

        legend: {
            show: true,
            live: false,
        },

        cursor: {
            sync: {
                key: 'fault-tolerance',
            },
            x: true,  // Show X-axis cursor line
            y: false, // Hide Y-axis cursor line
            drag: {
                x: false, // Disable drag zooming on X-axis
                y: false, // Disable drag zooming on Y-axis
            },
        },

        plugins: [
            {
                hooks: {
                    init: [
                        (u, opts, data) => {
                            // Create custom tooltip element
                            const tooltip = document.createElement('div');
                            tooltip.className = 'uplot-tooltip';
                            tooltip.style.cssText = `
                                position: absolute;
                                background: #45475a;
                                color: #cdd6f4;
                                padding: 8px 12px;
                                border-radius: 6px;
                                border: 1px solid #6c7086;
                                font-family: 'Red Hat Display';
                                font-size: 12px;
                                pointer-events: none;
                                z-index: 100;
                                display: none;
                                width: 60px;
                                white-space: pre-line;
                            `;
                            document.body.appendChild(tooltip);
                            u._tooltip = tooltip;
                        }
                    ],
                    setCursor: [
                        u => {
                            const tooltip = u._tooltip;
                            if (!tooltip) return;

                            const idx = u.cursor.idx;
                            if (idx === null || !data || !data[idx]) {
                                tooltip.style.display = 'none';
                                return;
                            }

                            const point = data[idx];
                            const rect = u.over.getBoundingClientRect();

                            // Check which series are visible
                            const tolerableFailuresVisible = u.series[1].show !== false;
                            const activeNodesVisible = u.series[2].show !== false;

                            // Find the appropriate data point based on cursor X position
                            const cursorX = u.cursor.left;
                            const cursorUserData = u.posToVal(cursorX, 'x');

                            // Find the data point at or to the left of cursor position
                            let targetPoint = null;
                            let targetIdx = -1;
                            let beyondFinalPoint = false;

                            for (let i = data.length - 1; i >= 0; i--) {
                                if (data[i].user_data_gb <= cursorUserData) {
                                    targetPoint = data[i];
                                    targetIdx = i;
                                    break;
                                }
                            }

                            if (!targetPoint) {
                                tooltip.style.display = 'none';
                                return;
                            }

                            // Check if cursor is beyond the final data point
                            const finalPoint = data[data.length - 1];
                            if (cursorUserData > finalPoint.user_data_gb) {
                                beyondFinalPoint = true;
                            }

                            // Check which series is closest to cursor Y position for that X point
                            const cursorY = u.cursor.top;
                            const tolerableFailuresY = u.valToPos(targetPoint.nodes_can_fail, 'y');
                            const activeNodesY = u.valToPos(targetPoint.active_nodes, 'y');

                            const distToTolerable = tolerableFailuresVisible ? Math.abs(cursorY - tolerableFailuresY) : Infinity;
                            const distToActive = activeNodesVisible ? Math.abs(cursorY - activeNodesY) : Infinity;

                            let content = '';

                            if (beyondFinalPoint) {
                                // Beyond the final data point - no storage available
                                content = `> ${formatStorageCapacity(finalPoint.user_data_gb)}\n\nOut of Storage`;
                            } else if (distToTolerable <= distToActive && tolerableFailuresVisible) {
                                // Hovering over tolerable failures line - ongoing state
                                content = `> ${formatStorageCapacity(targetPoint.user_data_gb)}\n\n`;
                                const activeNodes = targetPoint.participating_nodes;
                                let activeDisplay;
                                if (activeNodes.length > 5) {
                                    activeDisplay = `\n${activeNodes.length} nodes`;
                                } else {
                                    activeDisplay = activeNodes.map(n => n.display_name).join(', ');
                                }
                                content += `Can Fail: ${targetPoint.nodes_can_fail}\nActive: ${activeDisplay}`;
                            } else if (activeNodesVisible) {
                                // Hovering over active nodes line - transition event
                                content = `@ ${formatStorageCapacity(targetPoint.user_data_gb)}\n\n`;

                                if (targetIdx > 0) {
                                    const prevPoint = data[targetIdx - 1];
                                    const removedNodes = prevPoint.participating_nodes.filter(
                                        prevNode => !targetPoint.participating_nodes.find(
                                            currNode => currNode.node_id === prevNode.node_id
                                        )
                                    );

                                    if (removedNodes.length > 0) {
                                        let saturatedDisplay;
                                        if (removedNodes.length > 5) {
                                            saturatedDisplay = `${removedNodes.length} nodes`;
                                        } else {
                                            saturatedDisplay = removedNodes.map(n => n.display_name).join(', ');
                                        }
                                        content += `Saturated: ${saturatedDisplay}`;
                                    } else {
                                        let activeDisplay;
                                        if (targetPoint.participating_nodes.length > 5) {
                                            activeDisplay = `\n${targetPoint.participating_nodes.length} nodes`;
                                        } else {
                                            activeDisplay = targetPoint.participating_nodes.map(n => n.display_name).join(', ');
                                        }
                                        content += `Active: ${activeDisplay}`;
                                    }
                                } else {
                                    let activeDisplay;
                                    if (targetPoint.participating_nodes.length > 5) {
                                        activeDisplay = `\n${targetPoint.participating_nodes.length} nodes`;
                                    } else {
                                        activeDisplay = targetPoint.participating_nodes.map(n => n.display_name).join(', ');
                                    }
                                    content += `Active: ${activeDisplay}`;
                                }
                            } else {
                                // No visible series to show tooltip for
                                tooltip.style.display = 'none';
                                return;
                            }

                            tooltip.textContent = content;
                            tooltip.style.display = 'block';
                            // Position in top right of chart area
                            tooltip.style.left = (rect.right - 10) + 'px';
                            tooltip.style.top = (rect.top + 10) + 'px';
                            tooltip.style.transform = 'translateX(-100%)'; // Right-align to the position
                        }
                    ],
                    destroy: [
                        u => {
                            if (u._tooltip) {
                                u._tooltip.remove();
                            }
                        }
                    ]
                }
            }
        ],
    };

    // Convert curve data to uPlot format
    function convertDataForChart(points: FaultToleranceCurvePoint[]): uPlot.AlignedData {
        if (points.length === 0) {
            return [[], [], []];
        }

        const xData = points.map(p => p.user_data_gb);
        const yData1 = points.map(p => p.nodes_can_fail); // Tolerable failures
        const yData2 = points.map(p => p.active_nodes); // Active nodes

        return [xData, yData1, yData2];
    }

    // Detect over-capacity condition (network cannot accept new data)
    function isOverCapacity(points: FaultToleranceCurvePoint[]): boolean {
        return points.length === 1 &&
               points[0].user_data_gb === 0 &&
               points[0].active_nodes === 0 &&
               points[0].nodes_can_fail === 0 &&
               points[0].participating_nodes.length === 0;
    }

    // Create or update chart
    function updateChart() {
        if (!chartContainer) return;

        const chartData = convertDataForChart(data);

        if (chart) {
            // Update existing chart
            chart.setData(chartData);
        } else {
            // Create new chart
            chart = new uPlot(chartOptions, chartData, chartContainer);
        }
    }

    // Reactive update when data changes
    $: if (chartContainer && data && data.length > 0 && !isOverCapacity(data)) {
        updateChart();
    } else if (chart && (data.length === 0 || isOverCapacity(data))) {
        // Destroy chart when not needed
        chart.destroy();
        chart = null;
    }

    onMount(() => {
        updateChart();
    });

    onDestroy(() => {
        if (chart) {
            chart.destroy();
            chart = null;
        }
    });
</script>

<!-- Chart container with themed styling -->
<div class="bg-surface0 rounded-lg p-4 border border-overlay0">
    <div class="flex items-center justify-between mb-3">
        <h4 class="text-lg font-semibold text-primary">Storage Resilience</h4>
        <button
            class="text-sm bg-blue text-base px-3 py-1 rounded hover:bg-blue/80"
            onclick={onPlanClick}
        >
            {planButtonText}
        </button>
    </div>

    <!-- Chart container - always present but hidden when not needed -->
    <div class="flex justify-center" class:hidden={data.length === 0 || isOverCapacity(data)}>
        <div bind:this={chartContainer} class="chart-container"></div>
    </div>

    <!-- Legend/description - show with chart -->
    <div class="mt-3 text-sm text-muted space-y-1" class:hidden={data.length === 0 || isOverCapacity(data)}>
        <div class="flex items-center gap-2">
            <div class="w-3 h-0.5 bg-blue"></div>
            <span>Number of tolerable node failures before data loss</span>
        </div>
        <div class="flex items-center gap-2">
            <div class="w-3 h-0.5 bg-green border-dashed border-t-2 border-green bg-transparent"></div>
            <span>Total active nodes in the network</span>
        </div>
    </div>

    <!-- Over-capacity state -->
    <div class="text-center py-12" class:hidden={!isOverCapacity(data)}>
        <div class="i-carbon-warning-filled text-3xl mb-3 text-red mx-auto"></div>
        <h3 class="text-lg font-semibold text-red mb-2">Network Capacity Insufficient</h3>
        <p class="text-text mb-4">All nodes baseline usage exceeds the 90% operational safety threshold.</p>
        <div class="bg-surface1 rounded-lg p-4 text-left max-w-md mx-auto">
            <p class="text-sm text-subtitle mb-1"><strong>Single-node setups:</strong> Typical</p>
            <p class="text-sm text-subtitle mb-3"><strong>Mature deployments:</strong> Requires immediate attention</p>
            <p class="text-sm text-subtitle mb-2"><strong>Recommended actions:</strong></p>
            <ul class="text-sm text-muted space-y-1 ml-4">
                <li>Add more storage nodes to the network</li>
                <li>Increase storage capacity on existing nodes</li>
                <li>Clean up unused or duplicate data</li>
                <li>Review storage utilization patterns</li>
            </ul>
        </div>
    </div>

    <!-- Empty state -->
    <div class="text-muted text-center py-12" class:hidden={data.length > 0}>
        <div class="i-carbon-analytics text-2xl mb-2 opacity-50"></div>
        <p>No curve data to display</p>
        <p class="text-sm">Analyze a network configuration to see the fault tolerance curve</p>
    </div>
</div>

<style>
    /* Chart container styling */
    .chart-container {
        background: #313244; /* surface0 */
        border-radius: 0.5rem;
        overflow: hidden;
    }

    /* Override uPlot's default styling to match our theme */
    :global(.chart-container .u-legend) {
        background: #45475a !important; /* surface1 */
        border: 1px solid #6c7086 !important; /* overlay0 */
        border-radius: 0.375rem !important;
        color: #cdd6f4 !important; /* primary */
        font-family: 'Red Hat Display' !important;
    }

    :global(.chart-container .u-legend .u-marker) {
        background: transparent !important;
    }
</style>