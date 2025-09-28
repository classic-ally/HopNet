<script lang="ts">
    import { onMount, afterUpdate } from 'svelte';
    import Button from '../Button.svelte';
    import Tabs from './Tabs.svelte';

    // Action interface
    export interface ToolbarItem {
        type: 'action';
        icon: string;
        text: string;
        onClick: () => void;
        compactStage: number; // 0=never compact, higher=more willing
        tooltip?: string;
        disabled?: boolean;
        position?: 'left' | 'right';
    }

    // Tab interface
    export interface ToolbarTabs {
        type: 'tabs';
        tabs: Array<{
            key: string;
            label: string;
            icon?: string; // Required for compact mode
            color: 'red' | 'yellow' | 'green' | 'blue' | 'mauve';
        }>;
        activeTab: string;
        onTabChange?: (tab: string) => void;
        compactStage: number;
    }

    type ToolbarElement = ToolbarItem | ToolbarTabs;

    // Props
    export let mode: 'desktop' | 'mobile' = 'desktop';
    export let leftElements: ToolbarElement[] = [];
    export let centerElements: ToolbarElement[] = [];
    export let rightElements: ToolbarElement[] = [];
    export let className: string = '';

    // Internal state for compacting
    let toolbarContainer: HTMLDivElement;
    let compactedElements = new Set<ToolbarElement>(); // Track which elements are compacted
    let resizeObserver: ResizeObserver;
    let resizeTimeout: number;
    let isActivelyResizing = false;
    let resizeSettleTimeout: number;

    // Initialize compact state (start with desktop variants)
    function initializeCompactState() {
        compactedElements.clear();
    }

    // Get the variant for an element based on whether it's compacted
    function getElementVariant(element: ToolbarElement): 'desktop' | 'compact' | 'mobile' {
        if (mode === 'mobile') return 'mobile';

        if (element.type === 'action') {
            return compactedElements.has(element) ? 'compact' : 'desktop';
        } else {
            // For tabs, compact stage affects the tab display
            return 'desktop'; // Tabs component handles its own compacting
        }
    }

    // Get elements for a specific section (0=left, 1=center, 2=right)
    function getSectionElements(sectionIndex: number): ToolbarElement[] {
        switch (sectionIndex) {
            case 0: return leftElements;
            case 1: return centerElements;
            case 2: return rightElements;
            default: return [];
        }
    }

    // Measure section width by creating actual Button components with specified variants
    function measureSectionWithCompaction(sectionDiv: HTMLElement, sectionIndex: number, compactedStages: number[]): number {
        const sectionElements = getSectionElements(sectionIndex);

        // Create a temporary container with same styling as sections
        const measureContainer = document.createElement('div');
        measureContainer.style.cssText = `
            position: absolute;
            visibility: hidden;
            width: auto;
            max-width: none;
            display: flex;
            align-items: center;
            gap: 0.25rem;
            white-space: nowrap;
            top: -9999px;
            left: -9999px;
        `;

        // For each element in this section, create the actual component
        sectionElements.forEach((element) => {
            if (element.type === 'action') {
                // Determine what variant this element should use
                const shouldBeCompacted = compactedStages.includes(element.compactStage);
                const variant = shouldBeCompacted ? 'compact' : 'desktop';

                // Create a temporary Button component
                const buttonElement = document.createElement('button');

                // Apply the same classes as our Button component
                const sizeClasses = {
                    desktop: 'p-1 text-sm gap-1',
                    compact: 'p-[5px]',
                    mobile: 'p-2.5'
                }[variant];

                const iconSizeClass = {
                    desktop: 'text-lg',
                    compact: 'text-lg',
                    mobile: 'text-2xl'
                }[variant];

                buttonElement.className = `text-primary justify-center flex bg-surface1 border-overlay1 border-solid border-1 rounded-md items-center transition-colors whitespace-nowrap ${sizeClasses}`;

                // Create content based on variant
                if (variant === 'desktop') {
                    buttonElement.innerHTML = `
                        <div class="${element.icon} ${iconSizeClass}"></div>
                        <p>${element.text}</p>
                    `;
                } else {
                    // Compact variant - icon only
                    buttonElement.innerHTML = `
                        <div class="${element.icon} ${iconSizeClass}"></div>
                    `;
                }

                measureContainer.appendChild(buttonElement);
            }
            // TODO: Handle tabs if needed
        });

        document.body.appendChild(measureContainer);
        const width = measureContainer.getBoundingClientRect().width;
        document.body.removeChild(measureContainer);
        return width;
    }

    // Measure natural unconstrained width of a section
    function measureUnconstrainedSectionWidth(sectionDiv: HTMLElement, sectionIndex: number): number {
        return measureSectionWithCompaction(sectionDiv, sectionIndex, []); // No compaction applied
    }

    // Progressive compacting algorithm with per-section simulation
    function progressiveCompact() {
        if (!toolbarContainer) {
            return;
        }

        const containerWidth = toolbarContainer.clientWidth;
        const sections = Array.from(toolbarContainer.children) as HTMLElement[];

        let anyChanges = false;
        const newCompactedElements = new Set<ToolbarElement>();

        // Calculate optimal compaction level for each section
        for (let sectionIndex = 0; sectionIndex < sections.length; sectionIndex++) {
            const section = sections[sectionIndex];

            // Calculate actual available width per section accounting for CSS Grid gaps and padding
            // p-2 = 8px padding * 2 sides = 16px total padding
            // gap-1 = 4px gap * 2 gaps between 3 columns = 8px total gaps
            const totalPadding = 16;
            const totalGaps = 8;
            const availableForContent = containerWidth - totalPadding - totalGaps;
            const allocatedWidth = availableForContent / 3;
            const sectionName = sectionIndex === 0 ? 'Left' : sectionIndex === 1 ? 'Center' : 'Right';
            const sectionElements = getSectionElements(sectionIndex);

            // Get all available compact stages for this section
            const compactStages = [...new Set(sectionElements.map(el => el.compactStage))].filter(stage => stage > 0).sort((a, b) => b - a);

            // Start with no compaction and progressively test more compaction
            let optimalCompactionStages: number[] = [];

            // Test no compaction first
            const noCompactionWidth = measureSectionWithCompaction(section, sectionIndex, []);

            if (noCompactionWidth <= allocatedWidth) {
                optimalCompactionStages = [];
            } else {
                // Test progressive compaction levels
                let foundSolution = false;
                for (let i = 0; i < compactStages.length && !foundSolution; i++) {
                    const stagesToTest = compactStages.slice(0, i + 1);
                    const simulatedWidth = measureSectionWithCompaction(section, sectionIndex, stagesToTest);

                    if (simulatedWidth <= allocatedWidth) {
                        optimalCompactionStages = stagesToTest;
                        foundSolution = true;
                    }
                }

                if (!foundSolution) {
                    optimalCompactionStages = compactStages; // Use maximum compaction
                }
            }

            // Add optimally compacted elements to new set
            sectionElements.forEach(element => {
                if (element.type === 'action' && optimalCompactionStages.includes(element.compactStage)) {
                    newCompactedElements.add(element);
                }
            });
        }

        // Compare with current state to detect changes
        const currentCompactedArray = Array.from(compactedElements);
        const newCompactedArray = Array.from(newCompactedElements);

        // Check if sets are different
        if (currentCompactedArray.length !== newCompactedArray.length ||
            !currentCompactedArray.every(el => newCompactedElements.has(el)) ||
            !newCompactedArray.every(el => compactedElements.has(el))) {
            anyChanges = true;
        }

        // Apply the new optimal compaction state with conservative bias during active resize
        if (anyChanges) {
            if (isActivelyResizing) {
                // During active resize: only apply if new state is more conservative (more compacted)
                if (newCompactedArray.length >= currentCompactedArray.length) {
                    compactedElements = newCompactedElements;
                }
                // Block expansion during active resize
            } else {
                // Normal operation: apply optimal state without bias
                compactedElements = newCompactedElements;
            }
        }
    }

    // Handle tab changes
    function handleTabChange(tabsElement: ToolbarTabs, newTab: string) {
        if (tabsElement.onTabChange) {
            tabsElement.onTabChange(newTab);
        }
    }

    // Setup ResizeObserver
    onMount(() => {
        initializeCompactState();

        if (toolbarContainer) {
            resizeObserver = new ResizeObserver((entries) => {
                // Track active resize state for conservative bias
                isActivelyResizing = true;
                clearTimeout(resizeSettleTimeout);
                resizeSettleTimeout = setTimeout(() => {
                    isActivelyResizing = false;
                    // Run one final compaction after resize settles without conservative bias
                    progressiveCompact();
                }, 50); // Allow expansion only after resize has settled

                // Debounce rapid resize events to prevent flicker during drag
                clearTimeout(resizeTimeout);
                resizeTimeout = setTimeout(() => {
                    progressiveCompact();
                }, 16); // ~60fps, balances responsiveness vs stability
            });
            resizeObserver.observe(toolbarContainer);
        }

        return () => {
            if (resizeObserver) {
                resizeObserver.disconnect();
            }
            clearTimeout(resizeTimeout);
            clearTimeout(resizeSettleTimeout);
        };
    });

    // Note: Removed afterUpdate() compaction to prevent ResizeObserver loops
    // ResizeObserver handles all necessary re-compaction automatically

    // Re-initialize when elements change
    $: if (leftElements || centerElements || rightElements) {
        initializeCompactState();
    }

    // Ensure Svelte tracks compactedElements changes for reactivity
    $: compactedElements && null;

</script>

<div
    bind:this={toolbarContainer}
    class="grid grid-cols-3 items-center gap-1 p-2 bg-surface0 rounded-lg {className}"
>
    <!-- Left Section -->
    <div class="flex items-center gap-1 justify-start">
        {#each leftElements as element}
            {#if element.type === 'action'}
                <Button
                    icon={element.icon}
                    text={element.text}
                    onClick={element.onClick}
                    variant={getElementVariant(element)}
                    tooltip={element.tooltip}
                    disabled={element.disabled}
                    position={element.position}
                />
            {:else if element.type === 'tabs'}
                <Tabs
                    tabs={element.tabs}
                    bind:activeTab={element.activeTab}
                    centered={false}
                />
            {/if}
        {/each}
    </div>

    <!-- Center Section -->
    <div class="flex items-center gap-1 justify-center">
        {#each centerElements as element}
            {#if element.type === 'action'}
                <Button
                    icon={element.icon}
                    text={element.text}
                    onClick={element.onClick}
                    variant={getElementVariant(element)}
                    tooltip={element.tooltip}
                    disabled={element.disabled}
                    position={element.position}
                />
            {:else if element.type === 'tabs'}
                <Tabs
                    tabs={element.tabs}
                    bind:activeTab={element.activeTab}
                    centered={false}
                />
            {/if}
        {/each}
    </div>

    <!-- Right Section -->
    <div class="flex items-center gap-1 justify-end">
        {#each rightElements as element}
            {#if element.type === 'action'}
                <Button
                    icon={element.icon}
                    text={element.text}
                    onClick={element.onClick}
                    variant={getElementVariant(element)}
                    tooltip={element.tooltip}
                    disabled={element.disabled}
                    position={element.position}
                />
            {:else if element.type === 'tabs'}
                <Tabs
                    tabs={element.tabs}
                    bind:activeTab={element.activeTab}
                    centered={false}
                />
            {/if}
        {/each}
    </div>
</div>