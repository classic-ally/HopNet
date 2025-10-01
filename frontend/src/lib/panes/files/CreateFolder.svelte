<script lang="ts">
    import { API_BASE_URL, tokenStore, currentPathStore } from '../../stores';
    import Button from '../../Button.svelte';
    import Modal from '../../primitives/Modal.svelte';
    import TextInput from '../../primitives/TextInput.svelte';

    // Props for external control
    interface CreateFolderProps {
        isOpen?: boolean;
        onClose?: () => void;
        onFolderCreated?: () => void;
        onError?: (error: string) => void;
    }

    let {
        isOpen = false,
        onClose = () => {},
        onFolderCreated = () => {},
        onError = () => {}
    }: CreateFolderProps = $props();

    // Get token and current path from stores
    const token = $derived($tokenStore);
    const currentPath = $derived($currentPathStore);

    let folderName = $state('');
    let isCreating = $state(false);
    let createError = $state('');

    function validateFolderName(name: string): boolean {
        // Check if folder name is not empty and doesn't contain invalid characters
        if (!name.trim()) return false;
        // Prevent names with slashes or other problematic characters
        if (name.includes('/') || name.includes('\\') || name.includes('..')) return false;
        return true;
    }

    function handleClose() {
        folderName = '';
        createError = '';
        onClose();
    }

    async function createFolder() {
        if (!validateFolderName(folderName)) {
            const error = 'Please enter a valid folder name (no slashes or special characters)';
            createError = error;
            onError(error);
            return;
        }

        if (!token) {
            const error = 'No authentication token found';
            createError = error;
            onError(error);
            return;
        }

        isCreating = true;
        createError = '';

        try {
            // Construct the full path for the new folder
            const folderPath = currentPath === '/' ? `/${folderName}` : `${currentPath}/${folderName}`;

            // Create FormData with just the path (no files = folder creation)
            const formData = new FormData();
            formData.append('path', folderPath);

            const response = await fetch(`${API_BASE_URL}/files`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                },
                body: formData
            });

            if (!response.ok) {
                throw new Error(`Folder creation failed: ${response.status} ${response.statusText}`);
            }

            // Clear form and close modal after successful creation
            setTimeout(() => {
                folderName = '';
                onFolderCreated();
                handleClose();
            }, 500);

        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : 'Folder creation failed';
            createError = errorMessage;
            onError(errorMessage);
        } finally {
            isCreating = false;
        }
    }

    // Handle Enter key in text input
    function handleKeydown(event: KeyboardEvent) {
        if (event.key === 'Enter' && !isCreating && validateFolderName(folderName)) {
            createFolder();
        }
    }

    // Expose create function for external control
    export function triggerCreate() {
        createFolder();
    }

    export function isValid() {
        return validateFolderName(folderName);
    }

    export function clearForm() {
        folderName = '';
        createError = '';
    }
</script>

{#if isOpen}
<Modal title="Create New Folder" onClose={handleClose}>
    {#snippet content()}
        <!-- Folder Name Input -->
        <div class="space-y-2">
            <label for="folder-name" class="block text-sm font-medium text-subtitle">
                Folder Name
            </label>
            <TextInput
                id="folder-name"
                value={folderName}
                placeholder="Enter folder name"
                disabled={isCreating}
                onkeydown={handleKeydown}
                oninput={(e: Event) => folderName = (e.target as HTMLInputElement).value}
            />
        </div>

        <!-- Error Message -->
        {#if createError}
            <div class="bg-red/20 border border-red rounded p-3">
                <p class="text-red text-sm">{createError}</p>
            </div>
        {/if}

        <!-- Success Indicator -->
        {#if isCreating}
            <div class="bg-mauve/20 border border-mauve rounded p-3">
                <p class="text-mauve text-sm">Creating folder...</p>
            </div>
        {/if}
    {/snippet}

    {#snippet footer()}
        <div class="flex items-center justify-between">
            <p class="text-xs text-muted">Create in: {currentPath}</p>
            <Button
                icon="i-carbon-folder-add"
                text={isCreating ? 'Creating...' : 'Create Folder'}
                onClick={createFolder}
                disabled={!validateFolderName(folderName) || isCreating}
            />
        </div>
    {/snippet}
</Modal>
{/if}