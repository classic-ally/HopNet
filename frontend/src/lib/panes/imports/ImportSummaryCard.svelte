<script lang="ts">
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
</script>

{#if counts}
    <div class="bg-surface0 border border-overlay0 rounded-lg p-4 space-y-3">
        <div class="flex items-center gap-3">
            <div class="{isComplete ? 'i-carbon-checkmark-filled text-green' : 'i-carbon-warning-filled text-red'} text-2xl flex-shrink-0"></div>
            <div class="flex-1">
                <h4 class="font-medium text-primary">
                    {isComplete ? 'Import complete' : 'Import failed'}
                </h4>
                <p class="text-sm text-muted">
                    {counts.imported} of {counts.total} files imported.
                    {#if counts.failed > 0}{counts.failed} failed.{/if}
                    {#if counts.skipped > 0}{counts.skipped} skipped.{/if}
                </p>
            </div>
        </div>
        {#if failedByCode.length > 0}
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
        {/if}
    </div>
{:else}
    <!-- Non-owner — terminal status known but no counts -->
    <div class="bg-surface0 border border-overlay0 rounded-lg p-4 flex items-center gap-3">
        <div class="{isComplete ? 'i-carbon-checkmark-filled text-green' : 'i-carbon-warning-filled text-red'} text-2xl flex-shrink-0"></div>
        <div>
            <h4 class="font-medium text-primary">{isComplete ? 'Import complete' : 'Import failed'}</h4>
            <p class="text-sm text-muted">Detailed counts only available on the owner device.</p>
        </div>
    </div>
{/if}
