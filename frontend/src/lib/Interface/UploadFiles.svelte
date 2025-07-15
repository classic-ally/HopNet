<script lang="ts">
    import { createEventDispatcher } from 'svelte';
    import { API_BASE_URL, tokenStore, currentPathStore } from '../stores';
    
    export let isOpen = false;
    
    const dispatch = createEventDispatcher();
    
    // Get token and current path from stores
    $: token = $tokenStore;
    $: currentPath = $currentPathStore;
    
    let files: File[] = [];
    let isDragOver = false;
    let isUploading = false;
    let uploadProgress = 0;
    let uploadError = '';
    
    function closePopover() {
        isOpen = false;
        dispatch('close');
    }
    
    function handleDragOver(event: DragEvent) {
        event.preventDefault();
        isDragOver = true;
    }
    
    function handleDragLeave(event: DragEvent) {
        event.preventDefault();
        isDragOver = false;
    }
    
    function handleDrop(event: DragEvent) {
        event.preventDefault();
        isDragOver = false;
        
        const droppedFiles = Array.from(event.dataTransfer?.files || []);
        files = [...files, ...droppedFiles];
    }
    
    function handleFileInput(event: Event) {
        const target = event.target as HTMLInputElement;
        const selectedFiles = Array.from(target.files || []);
        files = [...files, ...selectedFiles];
    }
    
    function removeFile(index: number) {
        files = files.filter((_, i) => i !== index);
    }
    
    function formatFileSize(bytes: number): string {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
    
    async function uploadFiles() {
        if (files.length === 0) return;
        
        if (!token) {
            uploadError = 'No authentication token found';
            return;
        }
        
        isUploading = true;
        uploadError = '';
        uploadProgress = 0;
        
        try {
            const formData = new FormData();
            
            // Add path parameter (use current browse path)
            formData.append('path', currentPath);
            
            // Add all files with size encoded in the field name
            files.forEach(file => {
                formData.append(`file_${file.size}`, file);
            });
            
            const response = await fetch(`${API_BASE_URL}/files`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                },
                body: formData
            });
            
            if (!response.ok) {
                throw new Error(`Upload failed: ${response.status} ${response.statusText}`);
            }
            
            uploadProgress = 100;
            
            // Clear files and close popover after successful upload
            setTimeout(() => {
                files = [];
                closePopover();
                dispatch('uploaded');
            }, 1000);
            
        } catch (error) {
            uploadError = error instanceof Error ? error.message : 'Upload failed';
        } finally {
            isUploading = false;
        }
    }
    
    function handleKeydown(event: KeyboardEvent) {
        if (event.key === 'Escape') {
            closePopover();
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
    <div class="fixed top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 bg-gray-800 border border-gray-600 rounded-lg shadow-xl z-50 w-96 max-w-[90vw] max-h-[80vh] overflow-hidden">
        <!-- Header -->
        <div class="flex items-center justify-between p-4 border-b border-gray-600">
            <h3 class="text-lg font-semibold text-white">Upload Files</h3>
            <button 
                class="text-gray-400 hover:text-white transition-colors"
                on:click={closePopover}
                aria-label="Close"
            >
                <div class="i-carbon-close text-xl"></div>
            </button>
        </div>
        
        <!-- Content -->
        <div class="p-4 space-y-4 max-h-[60vh] overflow-y-auto">
            <!-- Drop Zone -->
            <div 
                class="border-2 border-dashed rounded-lg p-6 text-center transition-colors {isDragOver ? 'border-indigo-400 bg-indigo-900/20' : 'border-gray-500 hover:border-gray-400'}"
                on:dragover={handleDragOver}
                on:dragleave={handleDragLeave}
                on:drop={handleDrop}
                role="button"
                tabindex="0"
                on:keydown={(e: KeyboardEvent) => e.key === 'Enter' && document.getElementById('file-input')?.click()}
            >
                <div class="i-carbon-cloud-upload text-4xl text-gray-400 mx-auto mb-2"></div>
                <p class="text-gray-300 mb-2">Drag and drop files here</p>
                <p class="text-gray-500 text-sm mb-3">or</p>
                <label for="file-input" class="inline-block bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded cursor-pointer transition-colors">
                    Choose Files
                </label>
                <input 
                    id="file-input"
                    type="file" 
                    multiple 
                    class="hidden"
                    on:change={handleFileInput}
                />
            </div>
            
            <!-- File List -->
            {#if files.length > 0}
                <div class="space-y-2">
                    <h4 class="text-sm font-medium text-gray-300">Selected Files ({files.length})</h4>
                    <div class="space-y-1 max-h-40 overflow-y-auto">
                        {#each files as file, index}
                            <div class="flex items-center justify-between bg-gray-700 rounded p-2">
                                <div class="flex-1 min-w-0">
                                    <p class="text-sm text-white truncate">{file.name}</p>
                                    <p class="text-xs text-gray-400">{formatFileSize(file.size)}</p>
                                </div>
                                <button 
                                    class="text-gray-400 hover:text-red-400 ml-2 transition-colors"
                                    on:click={() => removeFile(index)}
                                    aria-label="Remove file"
                                >
                                    <div class="i-carbon-trash-can text-sm"></div>
                                </button>
                            </div>
                        {/each}
                    </div>
                </div>
            {/if}
            
            <!-- Upload Progress -->
            {#if isUploading}
                <div class="space-y-2">
                    <div class="flex items-center justify-between">
                        <span class="text-sm text-gray-300">Uploading...</span>
                        <span class="text-sm text-gray-300">{uploadProgress}%</span>
                    </div>
                    <div class="w-full bg-gray-600 rounded-full h-2">
                        <div 
                            class="bg-indigo-600 h-2 rounded-full transition-all duration-300"
                            style="width: {uploadProgress}%"
                        ></div>
                    </div>
                </div>
            {/if}
            
            <!-- Error Message -->
            {#if uploadError}
                <div class="bg-red-900/50 border border-red-500 rounded p-3">
                    <p class="text-red-300 text-sm">{uploadError}</p>
                </div>
            {/if}
        </div>
        
        <!-- Footer -->
        <div class="flex items-center justify-between p-4">
            <p class="text-xs text-gray-400">Upload path: {currentPath}</p>
            <div class="flex gap-2">
                <button 
                    class="px-4 py-2 bg-red-500 rounded-md border-none hover:bg-red-600 text-white transition-colors"
                    on:click={closePopover}
                    disabled={isUploading}
                >
                    Cancel
                </button>
                <button 
                    class="px-4 py-2 bg-indigo-600 rounded-md border-none hover:bg-indigo-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white rounded transition-colors"
                    on:click={uploadFiles}
                    disabled={files.length === 0 || isUploading}
                >
                    {isUploading ? 'Uploading...' : `Upload ${files.length} file${files.length !== 1 ? 's' : ''}`}
                </button>
            </div>
        </div>
    </div>
{/if}