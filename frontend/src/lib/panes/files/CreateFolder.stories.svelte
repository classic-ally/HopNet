<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import CreateFolder from './CreateFolder.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Files/CreateFolder',
        component: CreateFolder,
        argTypes: {
            isOpen: {
                control: 'boolean',
                description: 'Whether the create folder modal is open'
            },
            onClose: {
                action: 'onClose',
                description: 'Function called when modal is closed'
            },
            onFolderCreated: {
                action: 'onFolderCreated',
                description: 'Function called when folder is created successfully'
            },
            onError: {
                action: 'onError',
                description: 'Function called when folder creation encounters an error'
            }
        }
    });
</script>

<script lang="ts">
    function handleClose() {
        console.log('Story: Modal closed');
    }

    function handleFolderCreated() {
        console.log('Story: Folder created successfully');
    }

    function handleError(error: string) {
        console.log('Story: Folder creation error:', error);
    }
</script>

<!-- Create Folder Modal Story -->
<Story
    name="Create Folder Modal"
    args={{
        isOpen: true,
        onClose: handleClose,
        onFolderCreated: handleFolderCreated,
        onError: handleError
    }}
>
    {#snippet template(args)}
        <CreateFolder {...args} />
        <div class="mt-4 text-sm text-muted text-center">
            <p>• Toggle "isOpen" in the Controls tab to show/hide the modal</p>
            <p>• Type a folder name to see the button become enabled</p>
            <p>• Try invalid names (with / or \) to see validation</p>
            <p>• Check the Actions tab to see creation events</p>
        </div>
    {/snippet}
</Story>