// Column sizing for the Table primitive, ported from utils/tableColumns.ts.
// Same model: every column has a [min, max] range and a priority tier —
// higher tiers shrink first as the container narrows, tier 0 never shrinks,
// and absorbExcess columns soak up spare width on wide containers. The Table
// applies the result through <colgroup>, so the old nth-child selector loops
// and per-cell style writes are gone.

export interface ColumnSizing {
    min: number;
    max: number;
    /** Higher tiers shrink first; 0 never shrinks. */
    tier: number;
    /** Absorbs spare width when the container exceeds the total max. */
    absorbExcess?: boolean;
}

export const COLUMN_PRESETS = {
    icon: { min: 40, max: 40, tier: 0 },
    checkbox: { min: 40, max: 40, tier: 0 },
    uuid: { min: 100, max: 300, tier: 3 },
    date: { min: 96, max: 200, tier: 3 },
    size: { min: 95, max: 95, tier: 0 },
    status: { min: 80, max: 150, tier: 2 },
    name: { min: 200, max: 500, tier: 1, absorbExcess: true },
    path: { min: 150, max: 500, tier: 1, absorbExcess: true },
    description: { min: 200, max: 600, tier: 1, absorbExcess: true },
} as const satisfies Record<string, ColumnSizing>;

export type ColumnPreset = keyof typeof COLUMN_PRESETS;

/** Fallback for columns that name no preset: a flexible text column. */
export const DEFAULT_SIZING: ColumnSizing = COLUMN_PRESETS.name;

/**
 * Distribute `containerWidth` across the columns. Returns pixel widths
 * parallel to `sizings`; the caller renders them as <col> percentages so the
 * table always fills its container exactly.
 */
export function calculateColumnWidths(containerWidth: number, sizings: ColumnSizing[]): number[] {
    const totalMin = sizings.reduce((sum, s) => sum + s.min, 0);
    const totalMax = sizings.reduce((sum, s) => sum + s.max, 0);

    // Too narrow: everything at minimum, the table scrolls horizontally.
    if (containerWidth <= totalMin) return sizings.map((s) => s.min);

    // Wider than every maximum: spare width goes to the absorbers.
    if (containerWidth >= totalMax) {
        const absorbers = sizings.filter((s) => s.absorbExcess).length;
        const excess = absorbers > 0 ? (containerWidth - totalMax) / absorbers : 0;
        return sizings.map((s) => s.max + (s.absorbExcess ? excess : 0));
    }

    // In between: start at max, shrink tier by tier (highest first) until the
    // deficit is absorbed. Within a tier, shrink proportionally to each
    // column's available range.
    const widths = sizings.map((s) => s.max);
    let currentTotal = totalMax;
    const maxTier = Math.max(...sizings.map((s) => s.tier));

    for (let tier = maxTier; tier >= 0 && currentTotal > containerWidth; tier--) {
        const tierIdx = sizings.flatMap((s, i) => (s.tier === tier ? [i] : []));
        if (tierIdx.length === 0) continue;

        const deficit = currentTotal - containerWidth;
        const capacity = tierIdx.reduce((sum, i) => sum + (widths[i] - sizings[i].min), 0);

        if (capacity >= deficit) {
            for (const i of tierIdx) {
                const range = widths[i] - sizings[i].min;
                widths[i] -= capacity > 0 ? (range / capacity) * deficit : 0;
            }
            break;
        }

        for (const i of tierIdx) {
            currentTotal -= widths[i] - sizings[i].min;
            widths[i] = sizings[i].min;
        }
    }

    return widths;
}
