<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import Upload from './Upload.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Files/Upload',
        component: Upload,
        argTypes: {
            isOpen: {
                control: 'boolean',
                description: 'Whether the upload modal is open'
            },
            onClose: {
                action: 'onClose',
                description: 'Function called when modal is closed'
            },
            onUploadComplete: {
                action: 'onUploadComplete',
                description: 'Function called when upload completes successfully'
            },
            onError: {
                action: 'onError',
                description: 'Function called when upload encounters an error'
            },
            onProgress: {
                action: 'onProgress',
                description: 'Function called with upload progress updates'
            }
        }
    });
</script>

<script lang="ts">
    function handleClose() {
        console.log('Story: Modal closed');
    }

    function handleUploadComplete() {
        console.log('Story: Upload completed successfully');
    }

    function handleError(error: string) {
        console.log('Story: Upload error:', error);
    }

    function handleProgress(progress: number) {
        console.log('Story: Upload progress:', progress + '%');
    }
</script>

<!-- Upload Modal Story -->
<Story
    name="Upload Modal"
    args={{
        isOpen: true,
        onClose: handleClose,
        onUploadComplete: handleUploadComplete,
        onError: handleError,
        onProgress: handleProgress
    }}
>
    {#snippet template(args)}
        <Upload {...args} />
        <div class="mt-4 text-sm text-muted text-center">
            <p>• Toggle "isOpen" in the Controls tab to show/hide the modal</p>
            <p>• Drag and drop files onto the upload area</p>
            <p>• Or click "Choose Files" to select files</p>
            <p>• Check the Actions tab to see upload events</p>
        </div>
    {/snippet}
</Story>