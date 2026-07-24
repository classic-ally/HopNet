export type Band = 'Lazy' | 'Fast' | 'Cliff';

/**
 * Shared so the panel's sections cannot disagree about what a headroom value
 * means.
 *
 * `band` is taken from the payload rather than derived here: membership::band
 * is protocol policy — it scales t_probe, t_unresponsive and t_out by up to 4x
 * — so a copy of its thresholds in TypeScript would be the quorum-formula
 * drift again. The stalled split is added on top because Cliff spans h <= 0 and
 * so cannot distinguish "at the edge" from "already over it".
 */
export function headroomStatus(
    band: Band,
    headroom: number
): { label: string; tone: string; fill: string } {
    if (headroom < 0) return { label: 'Stalled', tone: 'text-red', fill: 'bg-red' };
    switch (band) {
        case 'Lazy':
            return { label: 'OK', tone: 'text-green', fill: 'bg-green' };
        case 'Fast':
            return { label: 'Low', tone: 'text-yellow', fill: 'bg-yellow' };
        default:
            return { label: 'Critical', tone: 'text-peach', fill: 'bg-peach' };
    }
}
