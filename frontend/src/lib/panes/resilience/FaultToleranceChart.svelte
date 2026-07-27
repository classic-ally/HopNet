<script lang="ts">
    import { onMount, onDestroy } from 'svelte';
    import uPlot from 'uplot';
    import 'uplot/dist/uPlot.min.css';
    import Button from '../../Button.svelte';
    import type { FaultToleranceCurvePoint } from '../../types';
    import { formatStorageCapacity } from '../../utils/formatters';

    export let data: FaultToleranceCurvePoint[] = [];

    // Actual placement as a sorted resilience frontier. One entry per
    // distinct tolerance level, from GROUP BY on the diagnostics query — the
    // curve is reconstructed by cumulative-summing in DESCENDING tolerance
    // order, which places every step at exactly the right x. Sorting means the
    // result is a pure step function: nothing is interleaved, so any slope
    // would be a bucketing artifact rather than a real intermediate state.
    //
    // rawGb must be raw user bytes, not post-erasure-coding, to share the
    // curve's x-axis (expansion factor N/K = 3).
    export let observedLevels: { tolerance: number; rawGb: number }[] = [];

    // Already lost, and not a low point on the resilience continuum — kept off
    // the curve so it cannot read as merely fragile.
    export let unrecoverableGb = 0;
    // No attestation data: an observability gap, not a durability state.
    export let unknownGb = 0;

    // F — nodes still in the storage member view but unreachable right now.
    // They are intact by the decay-tier definition, so their fragments still
    // count toward the frontier, which makes it optimistic about this instant.
    // Deliberately NOT departed nodes: those are already excluded from the
    // inventory, so counting them here would subtract the same loss twice.
    export let unreachableMembers = 0;

    // Zoom the x-axis to the current stored data volume rather than showing
    // the full ideal curve. Makes the observed frontier fill more of the
    // chart width, useful when stored data is far below capacity.
    export let zoomToData = false;

    // Total stored user bytes, derived from the observed frontier.
    $: consumedGb = frontier.reduce((a, s) => a + s.rawGb, 0);

    // Descending, so cumulative x is "how much data is at least this resilient".
    $: frontier = [...observedLevels].sort((a, b) => b.tolerance - a.tolerance);
    $: hasObserved = frontier.length > 0;

    // INV-DURABLE is a min-property: the worst block decides whether data is
    // lost, however healthy the rest are. On a sorted frontier that is simply
    // the last step.
    $: worstTolerance = hasObserved ? frontier[frontier.length - 1].tolerance : null;

    // Not guaranteed reconstructible while F holders are away. An upper bound
    // on damage, not a measurement: tolerance is the adversarial worst case.
    $: atRiskGb =
        unreachableMembers > 0
            ? frontier
                  .filter(s => s.tolerance < unreachableMembers)
                  .reduce((a, s) => a + s.rawGb, 0)
            : 0;

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
                    const max = zoomToData && consumedGb > 0 ? consumedGb : dataMax;
                    if (max <= 0 || !isFinite(max)) return [0, 1];
                    return [0, max + max / 20];
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

                    const tolerableFailuresVisible = u.series[1]?.show !== false;
                    const relevantPoints = [];

                    for (let i = 0; i < data.length; i++) {
                        const point = data[i];
                        if (point.user_data_gb < scaleMin || point.user_data_gb > scaleMax) continue;

                        let shouldShow = false;

                        if (i === 0 || i === data.length - 1) {
                            shouldShow = true;
                        } else {
                            const prevPoint = data[i - 1];
                            if (tolerableFailuresVisible && point.nodes_can_fail !== prevPoint.nodes_can_fail) {
                                shouldShow = true;
                            }
                        }

                        if (shouldShow) {
                            relevantPoints.push(point.user_data_gb);
                        }
                    }

                    if (zoomToData && consumedGb > 0 && consumedGb < scaleMax) {
                        relevantPoints.push(consumedGb);
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
                label: "Theoretical",
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
                                    const nextGradientPos = Math.min(1, (nextXPixel - plotLeft) / u.bbox.width);
                                    if (isFinite(nextGradientPos)) {
                                        const beforeNext = Math.max(gradientPos, nextGradientPos - 0.001);
                                        if (beforeNext > gradientPos && beforeNext <= 1) {
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

                    // Clip to plot area so off-screen segments under zoom
                    // do not bleed into the padding.
                    ctx.beginPath();
                    ctx.rect(u.bbox.left, u.bbox.top, u.bbox.width, u.bbox.height);
                    ctx.clip();

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
                },

                // The observed resilience frontier: data sorted best-placed
                // first, so the curve reads "this much data is at least this
                // resilient". Drawn last so the ideal curve's own segments
                // cannot paint over it, and reading props directly rather than
                // a derived flag so closure timing is not in question.
                u => {
                    if (!observedLevels || observedLevels.length === 0) return;

                    const steps = [...observedLevels].sort((a, b) => b.tolerance - a.tolerance);
                    const ctx = u.ctx;
                    ctx.save();

                    // Resolve each step's pixel extent once — both the frontier
                    // line and the at-risk band need the same geometry.
                    let cumulative = 0;
                    const spans: {
                        x0: number;
                        x1: number;
                        y: number;
                        tolerance: number;
                        rawGb: number;
                    }[] = [];
                    for (const step of steps) {
                        const x0 = u.valToPos(cumulative, 'x', true);
                        cumulative += step.rawGb;
                        const x1 = u.valToPos(cumulative, 'x', true);
                        const y = u.valToPos(step.tolerance, 'y', true);
                        if (!isFinite(x0) || !isFinite(x1) || !isFinite(y)) continue;
                        spans.push({ x0, x1, y, tolerance: step.tolerance, rawGb: step.rawGb });
                    }

                    // At-risk band: F holders are unreachable right now, so any
                    // block whose worst-case tolerance is below F is not
                    // GUARANTEED reconstructible until they return. Not a
                    // measurement of loss — tolerance is adversarial, so the
                    // specific offline nodes may hold few of a block's classes.
                    // The sound direction is the converse: tolerance >= F is
                    // definitely fine.
                    if (unreachableMembers > 0) {
                        const atRisk = spans.filter(s => s.tolerance < unreachableMembers);
                        if (atRisk.length > 0) {
                            // Clamp to the plot top: F can exceed the y-scale,
                            // which the Theoretical series alone determines.
                            const yF = Math.max(
                                u.bbox.top,
                                u.valToPos(unreachableMembers, 'y', true)
                            );

                            const tile = document.createElement('canvas');
                            tile.width = tile.height = 6;
                            const tctx = tile.getContext('2d');
                            if (tctx) {
                                tctx.strokeStyle = 'rgba(243, 139, 168, 0.55)';
                                tctx.lineWidth = 1;
                                tctx.beginPath();
                                tctx.moveTo(0, 6);
                                tctx.lineTo(6, 0);
                                tctx.stroke();
                            }
                            const pattern = ctx.createPattern(tile, 'repeat');

                            for (const s of atRisk) {
                                ctx.fillStyle = pattern ?? 'rgba(243, 139, 168, 0.2)';
                                ctx.fillRect(s.x0, yF, s.x1 - s.x0, s.y - yF);
                            }

                            // The F line itself, dashed so it never reads as a
                            // data series.
                            ctx.strokeStyle = '#f38ba8';
                            ctx.lineWidth = 1.5;
                            ctx.setLineDash([5, 4]);
                            ctx.beginPath();
                            ctx.moveTo(u.bbox.left, yF);
                            ctx.lineTo(u.bbox.left + u.bbox.width, yF);
                            ctx.stroke();
                            ctx.setLineDash([]);

                            // The band's WIDTH is the quantity; its area would
                            // be GB x nodes, which means nothing.
                            const gb = atRisk.reduce((a, s) => a + s.rawGb, 0);
                            ctx.font = '11px Red Hat Mono, monospace';
                            ctx.textAlign = 'center';
                            ctx.textBaseline = 'middle';

                            // Centre in the band: horizontally across its full
                            // extent, vertically between the F line and whatever
                            // step of the frontier sits under that midpoint —
                            // the region's floor is a staircase, not a flat edge.
                            const bandLeft = atRisk[0].x0;
                            const bandRight = atRisk[atRisk.length - 1].x1;
                            const midX = (bandLeft + bandRight) / 2;
                            const under =
                                atRisk.find(s => midX >= s.x0 && midX <= s.x1) ??
                                atRisk[atRisk.length - 1];
                            const midY = (yF + under.y) / 2;

                            // Break onto two lines when the band is too narrow
                            // to hold the whole phrase, so a thin band does not
                            // spill its label across the neighbouring curve.
                            const amount = formatStorageCapacity(gb);
                            const oneLine = `${amount} at risk`;
                            const lines =
                                ctx.measureText(oneLine).width + 10 > bandRight - bandLeft
                                    ? [amount, 'at risk']
                                    : [oneLine];

                            const lh = 13;
                            const boxH = lines.length * lh;
                            const boxW = Math.max(...lines.map(l => ctx.measureText(l).width));

                            // Knock the hatch out behind the text — at 11px the
                            // diagonals cut straight through the glyphs.
                            ctx.fillStyle = '#313244';
                            ctx.fillRect(midX - boxW / 2 - 4, midY - boxH / 2 - 2, boxW + 8, boxH + 4);

                            ctx.fillStyle = '#f38ba8';
                            lines.forEach((line, i) => {
                                ctx.fillText(line, midX, midY - boxH / 2 + lh * (i + 0.5));
                            });

                            ctx.textAlign = 'left';
                            ctx.textBaseline = 'alphabetic';
                        }
                    }

                    ctx.strokeStyle = '#cba6f7';
                    ctx.lineWidth = 2;
                    ctx.beginPath();

                    let prevY: number | null = null;
                    let endX: number | null = null;
                    let endY: number | null = null;

                    for (const s of spans) {
                        // Vertical drop into this level, then its run. Explicit
                        // rather than relying on a line-join, so a zero-width
                        // level still shows as a tick.
                        if (prevY === null) ctx.moveTo(s.x0, s.y);
                        else ctx.lineTo(s.x0, s.y);
                        ctx.lineTo(s.x1, s.y);
                        prevY = s.y;
                        endX = s.x1;
                        endY = s.y;
                    }

                    ctx.stroke();

                    // Terminus: where the frontier ends IS the total raw data
                    // on the network, so the dot doubles as that readout. Its
                    // y is the worst-placed block — the INV-DURABLE figure.
                    if (endX !== null && endY !== null) {
                        ctx.beginPath();
                        ctx.arc(endX, endY, 4, 0, Math.PI * 2);
                        ctx.fillStyle = '#cba6f7';
                        ctx.fill();

                        const label = formatStorageCapacity(cumulative);
                        ctx.font = '11px Red Hat Mono, monospace';
                        ctx.fillStyle = '#cdd6f4';
                        ctx.textBaseline = 'bottom';

                        // Flip the label inward near the right edge so it can
                        // never be clipped by the plot bounds.
                        const plotRight = u.bbox.left + u.bbox.width;
                        const w = ctx.measureText(label).width;
                        ctx.textAlign = endX + w + 10 > plotRight ? 'right' : 'left';
                        ctx.fillText(label, endX + (ctx.textAlign === 'right' ? -8 : 8), endY - 6);
                    }

                    ctx.restore();
                }
            ]
        },

        legend: {
            show: false,
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
                                font-family: 'Red Hat Display Variable', ui-sans-serif, system-ui, sans-serif;
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


                            let content = '';

                            if (beyondFinalPoint) {
                                // Beyond the final data point - no storage available
                                content = `> ${formatStorageCapacity(finalPoint.user_data_gb)}\n\nOut of Storage`;
                            } else if (tolerableFailuresVisible) {
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
        return [xData, yData1];
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

    // The observed marker lives in a draw hook, so a change to it needs an
    // explicit repaint — setData alone would not fire when only these move.
    //
    // Both args must be false. redraw()'s defaults are (true, true), and
    // recalcAxes re-runs the x range fn with null bounds, which returns
    // [null, NaN] and wipes the x scale — blanking the whole chart.
    $: if (chart && (observedLevels, unreachableMembers, true)) {
        if (zoomToData && consumedGb > 0) {
            updateChart();
        } else {
            chart.redraw(false, false);
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

<!-- Just the plot: the box, title and headline stat belong to StoragePanel,
     which composes this alongside UnplacedByAge. -->
<div>
    <!-- Chart container - always present but hidden when not needed -->
    <div class="flex justify-center" class:hidden={data.length === 0 || isOverCapacity(data)}>
        <div bind:this={chartContainer} class="chart-container"></div>
    </div>

    <!-- Legend/description - show with chart -->
    <div class="mt-3 text-sm text-muted space-y-1" class:hidden={data.length === 0 || isOverCapacity(data)}>
        <div class="flex items-center gap-2">
            <div class="w-3 h-0.5 bg-blue"></div>
            <span>Theoretical — tolerable node failures under ideal even spread</span>
        </div>
        {#if hasObserved}
            <div class="flex items-center gap-2">
                <div class="w-3 h-0.5 bg-mauve"></div>
                <span>Actual — data sorted best-placed first, on intact disks</span>
            </div>
        {/if}
        {#if atRiskGb > 0}
            <div class="flex items-center gap-2">
                <div class="w-3 h-0.5 border-t-2 border-dashed border-red"></div>
                <span>
                    {unreachableMembers} holder{unreachableMembers === 1 ? '' : 's'} unreachable —
                    hatched data is not guaranteed reconstructible
                </span>
            </div>
        {/if}
        {#if hasObserved}
            <div class="flex justify-end pt-1">
                <Button
                    icon="i-carbon-zoom-fit"
                    text={zoomToData ? 'Full view' : 'Fit to data'}
                    onClick={() => { zoomToData = !zoomToData; updateChart(); }}
                    tooltip={zoomToData ? 'Show full capacity range' : 'Fit x-axis to stored data'}
                />
            </div>
        {/if}
        {#if unrecoverableGb > 0 || unknownGb > 0}
            <div class="flex items-center gap-3 pt-1 font-mono text-xs">
                {#if unrecoverableGb > 0}
                    <span class="text-red">
                        {formatStorageCapacity(unrecoverableGb)} unrecoverable
                    </span>
                {/if}
                {#if unknownGb > 0}
                    <span
                        class="text-subtitle"
                        title="No attestation data — an observability gap, not a durability state"
                    >
                        {formatStorageCapacity(unknownGb)} never attested
                    </span>
                {/if}
            </div>
        {/if}
    </div>

    <!-- Over-capacity state -->
    <div class="text-center py-12" class:hidden={!isOverCapacity(data)}>
        <div class="i-carbon-warning-filled text-3xl mb-3 text-red mx-auto"></div>
        <h3 class="text-lg font-semibold text-red mb-2">Network Capacity Insufficient</h3>
        <p class="text-text mb-4">Every node has baseline usage exceeding 90%.</p>
        <div class="bg-base rounded-lg p-4 text-left max-w-md mx-auto">
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
        font-family: 'Red Hat Display Variable', ui-sans-serif, system-ui, sans-serif !important;
    }

    :global(.chart-container .u-legend .u-marker) {
        background: transparent !important;
    }
</style>