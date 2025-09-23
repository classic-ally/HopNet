/**
 * Svelte action for responsive table behavior based on column width
 * Adds CSS classes to the table element based on the width of specific cell types
 */

export interface ResponsiveBreakpoints {
  /** Width threshold for compact display (hide full timestamps, show date+time) */
  compact: number
  /** Width threshold for mini display (hide date+time, show date only) */
  mini: number
}

export interface ResponsiveConfig {
  /** CSS selector for cells to measure (e.g., '.date-cell') */
  cellSelector: string
  /** Breakpoint configuration */
  breakpoints: ResponsiveBreakpoints
  /** CSS class prefix for responsive classes (e.g., 'date' becomes 'date-compact', 'date-mini') */
  classPrefix: string
}

/**
 * Svelte action that makes tables responsive by measuring cell widths
 * and applying CSS classes for different breakpoints
 *
 * @param tableNode The table element to observe
 * @param config Configuration for cell selector, breakpoints, and class names
 * @returns Svelte action with cleanup function
 */
export function tableResponsive(tableNode: HTMLElement, config: ResponsiveConfig) {
  const { cellSelector, breakpoints, classPrefix } = config
  const compactClass = `${classPrefix}-compact`
  const miniClass = `${classPrefix}-mini`

  function updateResponsiveClasses() {
    const firstCell = tableNode.querySelector(cellSelector) as HTMLElement
    if (!firstCell) return

    const width = firstCell.clientWidth

    // Remove existing classes
    tableNode.classList.remove(compactClass, miniClass)

    // Add appropriate class based on width
    if (width < breakpoints.mini) {
      tableNode.classList.add(miniClass)
    } else if (width < breakpoints.compact) {
      tableNode.classList.add(compactClass)
    }
    // If width >= compact, no class is added (shows full format)
  }

  // Create ResizeObserver to watch for size changes
  const resizeObserver = new ResizeObserver(() => {
    updateResponsiveClasses()
  })

  // Initial measurement
  updateResponsiveClasses()

  // Start observing the table for size changes
  resizeObserver.observe(tableNode)

  return {
    // Update configuration if needed
    update(newConfig: ResponsiveConfig) {
      // Stop observing with old config
      resizeObserver.disconnect()

      // Update config
      Object.assign(config, newConfig)

      // Restart with new config
      updateResponsiveClasses()
      resizeObserver.observe(tableNode)
    },

    // Cleanup when component is destroyed
    destroy() {
      resizeObserver.disconnect()
    }
  }
}

/**
 * Pre-configured responsive action for date cells
 * Uses standard breakpoints for date formatting
 */
export function dateResponsive(tableNode: HTMLElement) {
  return tableResponsive(tableNode, {
    cellSelector: '.date-cell',
    breakpoints: {
      compact: 210, // Hide full timestamp, show date+time
      mini: 185     // Hide date+time, show date only
    },
    classPrefix: 'date'
  })
}

/**
 * Pre-configured responsive action for ID cells
 * Uses standard breakpoints for ID truncation
 */
export function idResponsive(tableNode: HTMLElement) {
  return tableResponsive(tableNode, {
    cellSelector: '.id-cell',
    breakpoints: {
      compact: 200, // Start truncating IDs
      mini: 120     // More aggressive truncation
    },
    classPrefix: 'id'
  })
}

/**
 * Responsive action for table padding based on container width
 * Reduces padding at smaller widths to maximize content space
 */
export function tablePaddingResponsive(tableNode: HTMLElement) {
  function updatePaddingClasses() {
    const width = tableNode.clientWidth

    // Remove all padding classes
    tableNode.classList.remove('padding-normal', 'padding-compact', 'padding-mini')

    // Add appropriate padding class based on table width
    if (width < 600) {
      tableNode.classList.add('padding-mini')      // Minimal padding
    } else if (width < 800) {
      tableNode.classList.add('padding-compact')   // Reduced padding
    } else {
      tableNode.classList.add('padding-normal')    // Normal padding
    }
  }

  const resizeObserver = new ResizeObserver(() => {
    updatePaddingClasses()
  })

  // Initial measurement
  updatePaddingClasses()

  // Start observing
  resizeObserver.observe(tableNode)

  return {
    destroy() {
      resizeObserver.disconnect()
    }
  }
}