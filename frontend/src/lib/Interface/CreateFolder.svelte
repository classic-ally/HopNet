<script lang="ts">
    import { createEventDispatcher } from 'svelte';
    import { API_BASE_URL, tokenStore, currentPathStore } from '../stores';
    import Button from '../Button.svelte';
    
    export let isOpen = false;
    
    const dispatch = createEventDispatcher();
    
    // Get token and current path from stores
    $: token = $tokenStore;
    $: currentPath = $currentPathStore;
    
    let folderName = '';
    let isCreating = false;
    let createError = '';
    
    function closePopover() {
        isOpen = false;
        folderName = '';
        createError = '';
        dispatch('close');
    }
    
    function validateFolderName(name: string): boolean {
        // Check if folder name is not empty and doesn't contain invalid characters
        if (!name.trim()) return false;
        // Prevent names with slashes or other problematic characters
        if (name.includes('/') || name.includes('\\') || name.includes('..')) return false;
        return true;
    }
    
    async function createFolder() {
        if (!validateFolderName(folderName)) {
            createError = 'Please enter a valid folder name (no slashes or special characters)';
            return;
        }
        
        if (!token) {
            createError = 'No authentication token found';
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
            
            // Clear form and close popover after successful creation
            setTimeout(() => {
                folderName = '';
                closePopover();
                dispatch('created');
            }, 500);
            
        } catch (error) {
            createError = error instanceof Error ? error.message : 'Folder creation failed';
        } finally {
            isCreating = false;
        }
    }
    
    function handleKeydown(event: KeyboardEvent) {
        if (event.key === 'Escape') {
            closePopover();
        } else if (event.key === 'Enter' && !isCreating && validateFolderName(folderName)) {
            createFolder();
        }
    }
    
    function handleInputKeydown(event: KeyboardEvent) {
        if (event.key === 'Enter' && !isCreating && validateFolderName(folderName)) {
            createFolder();
        }
    }
</script>

<svelte:window on:keydown={handleKeydown} />

{#if isOpen}
    <!-- Backdrop -->
    <div 
        class="fixed inset-0 bg-black bg-opacity-50 z-40"
        on:click={closePopover}
        role="button"
        tabindex="-1"
        on:keydown={(e: KeyboardEvent) => e.key === 'Enter' && closePopover()}
    ></div>
    
    <!-- Popover -->
    <div class="fixed top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 bg-surface0 border border-overlay1 rounded-lg shadow-xl z-50 w-full max-w-md mx-4 max-h-[80vh] overflow-hidden">
        <!-- Header -->
        <div class="flex items-center justify-between p-4 border-b border-overlay0">
            <h3 class="text-lg font-semibold text-white">Create New Folder</h3>
            <button 
                class="text-muted hover:text-primary transition-colors"
                on:click={closePopover}
                aria-label="Close"
            >
                <div class="i-carbon-close text-xl"></div>
            </button>
        </div>
        
        <!-- Content -->
        <div class="p-4 space-y-4">
            <!-- Folder Name Input -->
            <div class="space-y-2">
                <label for="folder-name" class="block text-sm font-medium text-subtitle">
                    Folder Name
                </label>
                <input 
                    id="folder-name"
                    type="text" 
                    class="w-full bg-surface1 border border-overlay1 rounded-lg px-3 py-2 text-primary placeholder-muted focus:outline-none focus:ring-2 focus:ring-mauve focus:border-transparent"
                    placeholder="Enter folder name"
                    bind:value={folderName}
                    on:keydown={handleInputKeydown}
                    disabled={isCreating}
                    autofocus
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
        </div>
        
        <!-- Footer -->
        <div class="flex items-center justify-between p-4 border-t border-overlay0 min-w-0">
            <p class="text-xs text-muted truncate mr-4 min-w-0 flex-1">Create in: {currentPath}</p>
            <Button
                icon="i-carbon-folder-add"
                text={isCreating ? 'Creating...' : 'Create Folder'}
                onClick={createFolder}
                disabled={!validateFolderName(folderName) || isCreating}
            />
        </div>
    </div>
{/if}