<script lang="ts">
    import Card from '../../primitives/Card.svelte';
    import type { ImportPathCounts, ImportStatus, ImportPathRow } from '../../types';

    interface Props {
        status: ImportStatus;        // 'Completed' | 'Failed'
        counts: ImportPathCounts | null;
        failedRows?: ImportPathRow[];
    }

    let { status, counts, failedRows = [] }: Props = $props();

    const failedByCode = $derived((() => {
        const map = new Map<string, number>();
        for (const r of failedRows) {
            const code = r.error_code ?? 'unknown';
            map.set(code, (map.get(code) ?? 0) + 1);
        }
        return Array.from(map.entries()).sort((a, b) => b[1] - a[1]);
    })());

    const isComplete = $derived(status === 'Completed');
    const icon = $derived(isComplete
        ? 'i-carbon-checkmark-filled text-green'
        : 'i-carbon-warning-filled text-red');
    const title = $derived(isComplete ? 'Import complete' : 'Import failed');

    const subtitle = $derived(counts
        ? `${counts.imported} of ${counts.total} files imported.`
            + (counts.failed > 0 ? ` ${counts.failed} failed.` : '')
            + (counts.skipped > 0 ? ` ${counts.skipped} skipped.` : '')
        : 'Detailed counts only available on the owner device.');
</script>

{#snippet failures()}
    <div class="border-t border-overlay0 pt-3">
        <h5 class="text-sm font-medium text-subtitle mb-2">Failures by reason</h5>
        <ul class="space-y-1 text-sm">
            {#each failedByCode as [code, count]}
                <li class="flex justify-between">
                    <span class="text-muted">{code}</span>
                    <span class="text-red">{count}</span>
                </li>
            {/each}
        </ul>
    </div>
{/snippet}

<!-- The snippet rides as a value so a failure-free summary is header-only,
     with no dangling header margin. -->
<Card {title} {subtitle} {icon} children={failedByCode.length > 0 ? failures : undefined} />
