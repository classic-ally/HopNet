import { describe, expect, it } from 'vitest';
import { COLUMN_PRESETS, calculateColumnWidths, type ColumnSizing } from './tablePresets';

const sum = (ns: number[]) => ns.reduce((a, b) => a + b, 0);

describe('calculateColumnWidths', () => {
    // Should: hold every column at its minimum when the container cannot fit them
    // Should not: shrink any column below its minimum
    it('floors at the minimums when too narrow', () => {
        const sizings: ColumnSizing[] = [
            { min: 100, max: 300, tier: 1 },
            { min: 100, max: 300, tier: 2 },
        ];
        expect(calculateColumnWidths(50, sizings)).toEqual([100, 100]);
    });

    // Should: give every column its maximum once the container exceeds their total
    // Should: split the spare width evenly between the absorbing columns
    // Should not: grow a column that does not absorb excess
    it('hands spare width to the absorbers', () => {
        const sizings: ColumnSizing[] = [
            { min: 40, max: 40, tier: 0 },
            { min: 100, max: 200, tier: 1, absorbExcess: true },
            { min: 100, max: 200, tier: 1, absorbExcess: true },
        ];
        const widths = calculateColumnWidths(640, sizings);
        expect(widths).toEqual([40, 300, 300]);
        expect(sum(widths)).toBe(640);
    });

    // Impact: without an absorber the table would be narrower than its
    // container, leaving a gap at the right edge of the tile.
    // Should not: distribute spare width when nothing absorbs it
    it('leaves excess unassigned with no absorber', () => {
        const sizings: ColumnSizing[] = [{ min: 40, max: 40, tier: 0 }];
        expect(calculateColumnWidths(500, sizings)).toEqual([40]);
    });

    // Impact: this is the whole point of the tier model — the name column must
    // survive a narrow window while the date column gives up its width first.
    // Should: shrink the highest tier before any lower tier
    // Should not: touch a tier-0 column while higher tiers still have room
    it('shrinks the highest tier first', () => {
        const sizings: ColumnSizing[] = [
            { min: 95, max: 95, tier: 0 },
            { min: 200, max: 400, tier: 1 },
            { min: 96, max: 200, tier: 3 },
        ];
        // Total max 695; at 600 the 95px deficit fits inside tier 3's 104px range.
        const widths = calculateColumnWidths(600, sizings);
        expect(widths[0]).toBe(95);
        expect(widths[1]).toBe(400);
        expect(widths[2]).toBe(105);
        expect(sum(widths)).toBe(600);
    });

    // Should: exhaust a tier down to its minimums before moving to the next
    // Should: land exactly on the container width
    it('cascades into lower tiers once a tier is exhausted', () => {
        const sizings: ColumnSizing[] = [
            { min: 95, max: 95, tier: 0 },
            { min: 200, max: 400, tier: 1 },
            { min: 96, max: 200, tier: 3 },
        ];
        // 695 - 400 = 295 deficit: tier 3 gives 104, tier 1 gives the rest.
        const widths = calculateColumnWidths(400, sizings);
        expect(widths[0]).toBe(95);
        expect(widths[2]).toBe(96);
        expect(sum(widths)).toBeCloseTo(400, 6);
    });

    // Should: split a tier's shrinkage in proportion to each column's range
    it('shrinks proportionally within a tier', () => {
        const sizings: ColumnSizing[] = [
            { min: 100, max: 200, tier: 1 },
            { min: 100, max: 300, tier: 1 },
        ];
        // Total max 500; a 60px deficit splits 1:2 across ranges of 100 and 200.
        const widths = calculateColumnWidths(440, sizings);
        expect(widths[0]).toBeCloseTo(180, 6);
        expect(widths[1]).toBeCloseTo(260, 6);
    });

    // Should: return one width per column, in the order given
    it('is parallel to its input', () => {
        const sizings = [COLUMN_PRESETS.checkbox, COLUMN_PRESETS.name, COLUMN_PRESETS.date];
        expect(calculateColumnWidths(800, sizings)).toHaveLength(3);
    });

    // Should: treat a container of zero width as too narrow rather than dividing by it
    it('survives a zero-width container', () => {
        const widths = calculateColumnWidths(0, [COLUMN_PRESETS.name]);
        expect(widths).toEqual([COLUMN_PRESETS.name.min]);
    });
});
