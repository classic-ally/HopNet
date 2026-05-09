<script lang="ts">
    import Button from '../../Button.svelte';

    interface Props {
        onSelect: (file: File) => void;
        uploading?: boolean;
        errorMessage?: string;
    }

    let { onSelect, uploading = false, errorMessage = undefined }: Props = $props();

    let isDragOver = $state(false);
    let fileInput: HTMLInputElement | undefined = $state(undefined);

    function handleDragOver(e: DragEvent) {
        e.preventDefault();
        isDragOver = true;
    }
    function handleDragLeave() { isDragOver = false; }
    function handleDrop(e: DragEvent) {
        e.preventDefault();
        isDragOver = false;
        const file = e.dataTransfer?.files?.[0];
        if (file) onSelect(file);
    }
    function handleFileInput(e: Event) {
        const input = e.target as HTMLInputElement;
        const file = input.files?.[0];
        if (file) onSelect(file);
        input.value = '';
    }
</script>

<div
    class="border-2 border-dashed rounded-lg p-8 text-center transition-colors {isDragOver ? 'border-mauve bg-mauve/20' : 'border-overlay1 hover:border-overlay2'}"
    ondragover={handleDragOver}
    ondragleave={handleDragLeave}
    ondrop={handleDrop}
    role="button"
    tabindex="0"
    onkeydown={(e) => e.key === 'Enter' && fileInput?.click()}
>
    <div class="i-carbon-cloud-upload text-5xl text-muted mx-auto mb-3"></div>
    <p class="text-subtitle mb-2">Drag a HopNet takeout archive here</p>
    <p class="text-muted text-sm mb-4">or</p>
    <div class="flex justify-center">
        <Button
            icon="i-carbon-document-add"
            text={uploading ? 'Uploading...' : 'Choose archive'}
            onClick={() => fileInput?.click()}
            disabled={uploading}
        />
    </div>
    <input
        bind:this={fileInput}
        type="file"
        accept=".tar.gz,.tgz,application/gzip,application/x-tar"
        class="hidden"
        onchange={handleFileInput}
    />
</div>

{#if errorMessage}
    <div class="bg-red/20 border border-red rounded p-3 mt-3">
        <p class="text-red text-sm">{errorMessage}</p>
    </div>
{/if}
