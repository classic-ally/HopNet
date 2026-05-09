<script lang="ts">
    import { importStatusStore, refreshCurrentUser } from '../../stores';
    import { uploadImport, fetchImportPaths } from '../../api/import';
    import { setOnboardingFlags } from '../../api/accounts';
    import { OnboardingFlag } from '../../types';
    import type { ImportPathRow } from '../../types';
    import Button from '../../Button.svelte';
    import ImportDropZone from '../imports/ImportDropZone.svelte';
    import ImportProgressCard from '../imports/ImportProgressCard.svelte';
    import ImportSummaryCard from '../imports/ImportSummaryCard.svelte';

    interface Props {
        onBack: () => void;
    }

    let { onBack }: Props = $props();

    let uploading = $state(false);
    let uploadError = $state('');
    let failedRows = $state<ImportPathRow[]>([]);
    let lastFailedFetchForId = $state<string | null>(null);

    const status = $derived($importStatusStore.record?.status);
    const counts = $derived($importStatusStore.counts);
    const isImporting = $derived(status === 'Pending' || status === 'Importing');
    const isTerminal = $derived(status === 'Completed' || status === 'Failed');

    // Fetch failed paths once per terminal record.
    $effect(() => {
        if (!isTerminal) return;
        const rec = $importStatusStore.record;
        if (!rec || lastFailedFetchForId === rec.id) return;
        lastFailedFetchForId = rec.id;
        fetchImportPaths()
            .then(rows => { failedRows = rows.filter(r => r.status === 'Failed'); })
            .catch(() => { failedRows = []; });
    });

    async function handleFile(file: File) {
        uploadError = '';
        uploading = true;
        try {
            // Mark as offered before uploading so abandoning mid-upload still
            // suppresses re-prompt; the actual flag is also auto-set by the
            // backend on terminal Completed.
            await setOnboardingFlags([OnboardingFlag.ImportOffered], []);
            await refreshCurrentUser();

            const result = await uploadImport(file);
            if (result.ok) {
                await importStatusStore.refresh();
            } else if (result.status === 507) {
                uploadError = `Archive too large for available quota. ${result.message}`;
            } else if (result.status === 409) {
                uploadError = 'An import is already in progress for this account.';
                await importStatusStore.refresh();
            } else {
                uploadError = `Upload failed (${result.status}): ${result.message}`;
            }
        } catch (e) {
            uploadError = e instanceof Error ? e.message : 'Upload failed';
        } finally {
            uploading = false;
        }
    }

    /// "Mark as done" — explicit user choice to dismiss this step without
    /// importing. Sets the ImportOffered bit so the checklist marks the step
    /// done. User can re-import later via... well, in v1 they can't, since
    /// HopNet doesn't support a second import per user.
    async function handleMarkDone() {
        try {
            await setOnboardingFlags([OnboardingFlag.ImportOffered], []);
            await refreshCurrentUser();
        } catch (_) { /* surface elsewhere */ }
        onBack();
    }
</script>

<div class="space-y-4">
    {#if isImporting}
        <ImportProgressCard {counts} />
        <p class="text-sm text-muted">
            You can close this dialog — the import continues in the background.
            We'll show progress in a banner at the top of the app.
        </p>
    {:else if isTerminal && status}
        <ImportSummaryCard {status} {counts} {failedRows} />
    {:else}
        <p class="text-sm text-subtitle">
            If you have a HopNet takeout archive (.tar.gz) from a previous installation,
            drop it here to restore your files.
        </p>
        <ImportDropZone onSelect={handleFile} {uploading} errorMessage={uploadError || undefined} />
        <div class="flex justify-end">
            <Button icon="i-carbon-checkmark" text="Mark as done" onClick={handleMarkDone} disabled={uploading} />
        </div>
    {/if}
</div>
