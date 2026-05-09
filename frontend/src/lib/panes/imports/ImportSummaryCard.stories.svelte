<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import ImportSummaryCard from './ImportSummaryCard.svelte';
    import { InodeType, ImportPathStatus, ImportStatus } from '../../types';

    const { Story } = defineMeta({
        title: 'Panes/Imports/ImportSummaryCard',
        component: ImportSummaryCard,
    });

    const failedRowsExample = [
        { path: '/photos/a.jpg', path_type: InodeType.File, size_bytes: '0', status: ImportPathStatus.Failed, error_code: 'hash_mismatch', error_message: '', processed_at: '' },
        { path: '/photos/b.jpg', path_type: InodeType.File, size_bytes: '0', status: ImportPathStatus.Failed, error_code: 'hash_mismatch', error_message: '', processed_at: '' },
        { path: '/photos/c.jpg', path_type: InodeType.File, size_bytes: '0', status: ImportPathStatus.Failed, error_code: 'hash_mismatch', error_message: '', processed_at: '' },
        { path: '/docs/d.pdf',   path_type: InodeType.File, size_bytes: '0', status: ImportPathStatus.Failed, error_code: 'fragment_distribution_failed', error_message: '', processed_at: '' },
        { path: '/docs/e.pdf',   path_type: InodeType.File, size_bytes: '0', status: ImportPathStatus.Failed, error_code: 'unknown', error_message: '', processed_at: '' },
    ];
</script>

<Story name="Completed — clean run" args={{
    status: ImportStatus.Completed,
    counts: { total: 15, pending: 0, imported: 15, skipped: 0, failed: 0 },
}}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportSummaryCard {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Completed — with failures" args={{
    status: ImportStatus.Completed,
    counts: { total: 100, pending: 0, imported: 92, skipped: 3, failed: 5 },
    failedRows: failedRowsExample,
}}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportSummaryCard {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Failed" args={{
    status: ImportStatus.Failed,
    counts: { total: 50, pending: 0, imported: 10, skipped: 0, failed: 40 },
    failedRows: failedRowsExample,
}}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportSummaryCard {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Non-owner (no counts)" args={{
    status: ImportStatus.Completed,
    counts: null,
}}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportSummaryCard {...args} />
        </div>
    {/snippet}
</Story>
