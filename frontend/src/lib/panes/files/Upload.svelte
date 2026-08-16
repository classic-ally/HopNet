<script lang="ts">
    import { API_BASE_URL, tokenStore, currentPathStore } from '../../stores';
    import Button from '../../Button.svelte';
    import Modal from '../../primitives/Modal.svelte';
    import Breadcrumb from '../../primitives/Breadcrumb.svelte';
    import { crumbsForFolder } from '../../primitives/crumbs';

    // Props for external control
    interface UploadProps {
        isOpen?: boolean;
        onClose?: () => void;
        onUploadComplete?: () => void;
        onError?: (error: string) => void;
        onProgress?: (progress: number) => void;
    }

    let {
        isOpen = false,
        onClose = () => {},
        onUploadComplete = () => {},
        onError = () => {},
        onProgress = () => {}
    }: UploadProps = $props();

    // Get token and current path from stores
    const token = $derived($tokenStore);
    const currentPath = $derived($currentPathStore);

    let files = $state<File[]>([]);
    let isDragOver = $state(false);
    let isUploading = $state(false);
    let uploadProgress = $state(0);
    let uploadError = $state('');

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
            const error = 'No authentication token found';
            uploadError = error;
            onError(error);
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
            onProgress(100);

            // Clear files and notify completion
            setTimeout(() => {
                files = [];
                onUploadComplete();
                onClose(); // Close modal after successful upload
            }, 1000);

        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : 'Upload failed';
            uploadError = errorMessage;
            onError(errorMessage);
        } finally {
            isUploading = false;
        }
    }

    // Expose the upload function and file state for parent components
    export function triggerUpload() {
        uploadFiles();
    }

    export function getFileCount() {
        return files.length;
    }

    export function hasFiles() {
        return files.length > 0;
    }

    export function clearFiles() {
        files = [];
    }
</script>

{#if isOpen}
<Modal title="Upload Files" {onClose}>
    {#snippet content()}
        <!-- Drop Zone -->
        <div
            class="border-2 border-dashed rounded-lg p-6 text-center transition-colors {isDragOver ? 'border-mauve bg-mauve/20' : 'border-overlay1 hover:border-overlay2'}"
            ondragover={handleDragOver}
            ondragleave={handleDragLeave}
            ondrop={handleDrop}
            role="button"
            tabindex="0"
            onkeydown={(e) => e.key === 'Enter' && document.getElementById('file-input')?.click()}
        >
            <div class="i-carbon-cloud-upload text-4xl text-muted mx-auto mb-2"></div>
            <p class="text-subtitle mb-2">Drag and drop files here</p>
            <p class="text-muted text-sm mb-3">or</p>
            <div class="flex justify-center">
                <Button
                    icon="i-carbon-document-add"
                    text="Choose Files"
                    onClick={() => document.getElementById('file-input')?.click()}
                />
            </div>
            <input
                id="file-input"
                type="file"
                multiple
                class="hidden"
                onchange={handleFileInput}
            />
        </div>

        <!-- File List -->
        {#if files.length > 0}
            <div class="space-y-2">
                <h4 class="text-sm font-medium text-subtitle">Selected Files ({files.length})</h4>
                <div class="space-y-1 max-h-40 overflow-y-auto">
                    {#each files as file, index}
                        <div class="flex items-center justify-between bg-surface1 rounded p-2">
                            <div class="flex-1 min-w-0">
                                <p class="text-sm text-white truncate">{file.name}</p>
                                <p class="text-xs text-muted">{formatFileSize(file.size)}</p>
                            </div>
                            <button
                                class="text-muted hover:text-red ml-2 transition-colors"
                                onclick={() => removeFile(index)}
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
                    <span class="text-sm text-subtitle">Uploading...</span>
                    <span class="text-sm text-subtitle">{uploadProgress}%</span>
                </div>
                <div class="w-full bg-surface1 rounded-full h-2">
                    <div
                        class="bg-mauve h-2 rounded-full transition-all duration-300"
                        style="width: {uploadProgress}%"
                    ></div>
                </div>
            </div>
        {/if}

        <!-- Error Message -->
        {#if uploadError}
            <div class="bg-red/20 border border-red rounded p-3">
                <p class="text-red text-sm">{uploadError}</p>
            </div>
        {/if}

    {/snippet}

    {#snippet footer()}
        <div class="flex items-center justify-between">
            <!-- Where the files land. Read-only for now; the same component
                 becomes the destination picker once it takes a callback. -->
            <Breadcrumb segments={crumbsForFolder(currentPath)} ariaLabel="Upload destination" />
            <Button
                icon="i-carbon-cloud-upload"
                text={isUploading ? 'Uploading...' : `Upload ${files.length} file${files.length !== 1 ? 's' : ''}`}
                onClick={uploadFiles}
                disabled={files.length === 0 || isUploading}
            />
        </div>
    {/snippet}
</Modal>
{/if}