<script lang="ts">
    import Button from '../../Button.svelte';
    import Modal from '../../primitives/Modal.svelte';
    import type { FileItem } from '../../types';
    import { InodeType } from '../../types';
    import { getFileName } from '../../utils/formatters';

    interface ConfirmDeleteProps {
        isOpen?: boolean;
        items?: FileItem[];
        onClose?: () => void;
        onConfirm?: () => void;
    }

    let {
        isOpen = false,
        items = [],
        onClose = () => {},
        onConfirm = () => {}
    }: ConfirmDeleteProps = $props();

    function handleClose() {
        onClose();
    }

    function handleConfirm() {
        onConfirm();
    }

    const fileCount = $derived(items.filter(item => item.inode_type === InodeType.File).length);
    const folderCount = $derived(items.filter(item => item.inode_type === InodeType.Folder).length);
</script>

{#if isOpen}
<Modal title="Confirm Deletion" onClose={handleClose} size="md">
    {#snippet content()}
        <div class="space-y-4">
            <div class="text-primary">
                Are you sure you want to delete the following items?
            </div>

            {#if items.length > 0}
                <div class="bg-surface0 border border-overlay0 rounded-lg p-3 max-h-48 overflow-y-auto">
                    <ul class="space-y-1">
                        {#each items as item}
                            <li class="text-sm text-subtitle flex items-center gap-2">
                                <div class="{item.inode_type === InodeType.Folder ? 'i-carbon-folder' : 'i-carbon-document'} w-4 h-4 flex-shrink-0"></div>
                                <span class="truncate">{getFileName(item.path)}</span>
                            </li>
                        {/each}
                    </ul>
                </div>

                <div class="bg-red/10 border border-red/30 rounded-lg p-3">
                    <div class="flex items-start gap-2">
                        <div class="i-carbon-warning text-red flex-shrink-0 mt-0.5"></div>
                        <div class="text-sm text-red">
                            <strong>Warning:</strong> This action cannot be undone.
                            {#if folderCount > 0}
                                Deleting folders will also delete all their contents.
                            {/if}
                        </div>
                    </div>
                </div>
            {/if}
        </div>
    {/snippet}

    {#snippet footer()}
        <div class="flex items-center justify-between w-full">
            <div class="text-sm text-muted">
                {#if fileCount > 0 && folderCount > 0}
                    {fileCount} {fileCount === 1 ? 'file' : 'files'} and {folderCount} {folderCount === 1 ? 'folder' : 'folders'}
                {:else if fileCount > 0}
                    {fileCount} {fileCount === 1 ? 'file' : 'files'}
                {:else if folderCount > 0}
                    {folderCount} {folderCount === 1 ? 'folder' : 'folders'}
                {/if}
            </div>
            <div class="flex items-center gap-2">
                <Button
                    icon="i-carbon-close"
                    text="Cancel"
                    onClick={handleClose}
                />
                <Button
                    icon="i-carbon-trash-can"
                    text="Delete"
                    onClick={handleConfirm}
                />
            </div>
        </div>
    {/snippet}
</Modal>
{/if}

<style>
    /* Custom scrollbar for the items list */
    .overflow-y-auto {
        scrollbar-width: thin;
        scrollbar-color: #45475a #313244;
    }

    .overflow-y-auto::-webkit-scrollbar {
        width: 6px;
    }

    .overflow-y-auto::-webkit-scrollbar-track {
        background: #313244;
        border-radius: 3px;
    }

    .overflow-y-auto::-webkit-scrollbar-thumb {
        background: #45475a;
        border-radius: 3px;
    }

    .overflow-y-auto::-webkit-scrollbar-thumb:hover {
        background: #585b70;
    }
</style>
