/**
 * Column configuration system for responsive tables
 * Allows defining column types with min/max widths and flexible sizing
 */

export interface ColumnConfig {
  /** Column identifier */
  id: string
  /** CSS selector for the column (e.g., 'nth-child(1)') */
  selector: string
  /** Column sizing with priority tier system */
  sizing: {
    min: number
    max: number
    /** Priority tier - higher numbers shrink first (0 = never shrinks) */
    tier: number
    /** If true, this column absorbs excess space when container exceeds total max */
    absorbExcess?: boolean
  }
}

export interface TableColumnsConfig {
  columns: ColumnConfig[]
  minTableWidth?: number
}

/**
 * Predefined column types for common use cases
 */
export const COLUMN_PRESETS = {
  icon: { min: 40, max: 40, tier: 0 },                                    // Never shrinks
  checkbox: { min: 40, max: 40, tier: 0 },                               // Never shrinks
  uuid: { min: 100, max: 300, tier: 3 },                                 // Shrinks first
  date: { min: 96, max: 200, tier: 3 },                                 // Shrinks first
  size: { min: 95, max: 95, tier: 0 },                                  // Never shrinks
  status: { min: 80, max: 150, tier: 2 },                                // Shrinks second
  name: { min: 200, max: 500, tier: 1, absorbExcess: true },             // Shrinks last
  path: { min: 150, max: 500, tier: 1, absorbExcess: true },             // Shrinks last
  description: { min: 200, max: 600, tier: 1, absorbExcess: true },      // Shrinks last
}

/**
 * Calculate optimal column widths based on container width
 * @param containerWidth Current container width
 * @param config Table column configuration
 * @returns Object with column selectors as keys and widths as values
 */
export function calculateColumnWidths(
  containerWidth: number,
  config: TableColumnsConfig
): Record<string, number> {
  const widths: Record<string, number> = {}

  // Calculate total min and max widths
  const totalMin = config.columns.reduce((sum, col) => sum + col.sizing.min, 0)
  const totalMax = config.columns.reduce((sum, col) => sum + col.sizing.max, 0)

  if (containerWidth <= totalMin) {
    // Container too small - use minimums, table becomes scrollable
    for (const column of config.columns) {
      widths[column.selector] = column.sizing.min
    }
  } else if (containerWidth >= totalMax) {
    // Container exceeds max total - use maximums, excess goes to absorbExcess columns
    const excessWidth = containerWidth - totalMax
    const absorbColumns = config.columns.filter(col => col.sizing.absorbExcess)
    const excessPerColumn = absorbColumns.length > 0 ? excessWidth / absorbColumns.length : 0

    for (const column of config.columns) {
      widths[column.selector] = column.sizing.max + (column.sizing.absorbExcess ? excessPerColumn : 0)
    }
  } else {
    // Priority tier shrinking system
    // Start all columns at max width
    const columnData = config.columns.map(col => ({
      ...col,
      currentWidth: col.sizing.max
    }))

    let currentTotalWidth = totalMax
    const maxTier = Math.max(...config.columns.map(col => col.sizing.tier))

    // Shrink tier by tier from highest to lowest
    for (let tier = maxTier; tier >= 0 && currentTotalWidth > containerWidth; tier--) {
      const tierColumns = columnData.filter(col => col.sizing.tier === tier)

      if (tierColumns.length === 0) continue

      const deficit = currentTotalWidth - containerWidth

      // Calculate how much this tier can shrink
      const tierShrinkCapacity = tierColumns.reduce((sum, col) =>
        sum + Math.max(0, col.currentWidth - col.sizing.min), 0)

      if (tierShrinkCapacity >= deficit) {
        // This tier can absorb all remaining deficit
        const totalRange = tierColumns.reduce((sum, col) =>
          sum + Math.max(0, col.currentWidth - col.sizing.min), 0)

        tierColumns.forEach(col => {
          const colRange = Math.max(0, col.currentWidth - col.sizing.min)
          const shrinkAmount = totalRange > 0 ? (colRange / totalRange) * deficit : 0
          col.currentWidth -= shrinkAmount
        })
        break
      } else {
        // Shrink this tier to minimums, continue to next tier
        tierColumns.forEach(col => {
          currentTotalWidth -= (col.currentWidth - col.sizing.min)
          col.currentWidth = col.sizing.min
        })
      }
    }

    // Set final widths
    for (const col of columnData) {
      widths[col.selector] = col.currentWidth
    }
  }

  return widths
}

/**
 * Apply responsive classes based on calculated column widths and container size
 * This integrates date formatting and padding responsive behavior
 */
function applyResponsiveClasses(
  tableNode: HTMLElement,
  calculatedWidths: Record<string, number>,
  containerWidth: number
) {
  // Find date column widths (nth-child(4) and nth-child(5) for created/modified)
  const dateWidth = Math.min(
    calculatedWidths['nth-child(4)'] || 0,
    calculatedWidths['nth-child(5)'] || 0
  )

  // Remove existing date responsive classes
  tableNode.classList.remove('date-compact', 'date-mini')

  // Apply date responsive classes based on calculated width
  if (dateWidth < 170) {
    tableNode.classList.add('date-mini')      // Show date only
  } else if (dateWidth < 200) {
    tableNode.classList.add('date-compact')   // Show date+time without seconds
  }
  // If dateWidth >= 210, no class = show full timestamp

  // Remove existing padding classes
  tableNode.classList.remove('padding-normal', 'padding-compact', 'padding-mini')

  // Apply padding classes based on container width
  if (containerWidth < 830) {
    tableNode.classList.add('padding-mini')      // Minimal padding
  } else if (containerWidth < 900) {
    tableNode.classList.add('padding-compact')   // Reduced padding
  } else {
    tableNode.classList.add('padding-normal')    // Normal padding
  }
}

/**
 * Svelte action for applying dynamic column widths
 * @param tableNode The table element to manage
 * @param config Column configuration
 */
export function tableColumns(tableNode: HTMLElement, config: TableColumnsConfig) {
  function updateColumnWidths() {
    // Get the actual container width (parent element), not the table width
    // The table might have min-width set, so we need the container to know actual available space
    const containerWidth = tableNode.parentElement?.clientWidth || tableNode.clientWidth
    const widths = calculateColumnWidths(containerWidth, config)


    // Calculate total width for horizontal scrolling
    const totalCalculatedWidth = Object.values(widths).reduce((sum, w) => sum + w, 0)

    // Apply widths as percentages for perfect container fill
    for (const [selector, width] of Object.entries(widths)) {
      const percentage = (width / totalCalculatedWidth) * 100
      const headers = tableNode.querySelectorAll(`th:${selector}`) as NodeListOf<HTMLElement>
      const cells = tableNode.querySelectorAll(`td:${selector}`) as NodeListOf<HTMLElement>


      headers.forEach(el => {
        el.style.width = `${percentage}%`
        el.style.maxWidth = `${percentage}%`
        el.style.minWidth = `${percentage}%`
      })

      // Apply to first row of cells
      if (cells.length > 0) {
        const cell = cells[0]
        cell.style.width = `${percentage}%`
        cell.style.maxWidth = `${percentage}%`
        cell.style.minWidth = `${percentage}%`
      }
    }

    // Set table width for scrolling behavior
    tableNode.style.width = totalCalculatedWidth < containerWidth ? '100%' : `${totalCalculatedWidth}px`

    // Set minimum width to enable horizontal scrolling when needed
    const minWidth = config.columns.reduce((sum, col) => sum + col.sizing.min, 0)
    tableNode.style.minWidth = `${minWidth}px`


    // Apply responsive formatting classes based on calculated widths, not measured widths
    applyResponsiveClasses(tableNode, widths, containerWidth)
  }

  const resizeObserver = new ResizeObserver(() => {
    updateColumnWidths()
  })

  // Initial calculation
  updateColumnWidths()

  // Start observing the parent container if it exists, otherwise the table
  const elementToObserve = tableNode.parentElement || tableNode
  resizeObserver.observe(elementToObserve)

  return {
    update(newConfig: TableColumnsConfig) {
      config = newConfig
      updateColumnWidths()
    },
    destroy() {
      resizeObserver.disconnect()
    }
  }
}

/**
 * Configuration for the file browser table
 */
export const fileBrowserColumns: TableColumnsConfig = {
  columns: [
    { id: 'type', selector: 'nth-child(1)', sizing: COLUMN_PRESETS.icon },
    { id: 'name', selector: 'nth-child(2)', sizing: COLUMN_PRESETS.name },
    { id: 'size', selector: 'nth-child(3)', sizing: COLUMN_PRESETS.size },
    { id: 'created', selector: 'nth-child(4)', sizing: COLUMN_PRESETS.date },
    { id: 'modified', selector: 'nth-child(5)', sizing: COLUMN_PRESETS.date },
  ]
}

/**
 * Configuration for the takeout table
 */
export const takeoutTableColumns: TableColumnsConfig = {
  columns: [
    { id: 'checkbox', selector: 'nth-child(1)', sizing: COLUMN_PRESETS.checkbox },
    { id: 'id', selector: 'nth-child(2)', sizing: COLUMN_PRESETS.uuid },
    { id: 'status', selector: 'nth-child(3)', sizing: COLUMN_PRESETS.status },
    { id: 'created', selector: 'nth-child(4)', sizing: COLUMN_PRESETS.date },
    { id: 'expires', selector: 'nth-child(5)', sizing: COLUMN_PRESETS.date },
  ]
}