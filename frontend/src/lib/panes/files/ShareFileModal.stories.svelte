<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import ShareFileModal from './ShareFileModal.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Files/ShareFileModal',
        component: ShareFileModal,
        argTypes: {
            isOpen: { control: 'boolean', description: 'Whether the modal is open' },
            onShare: { action: 'onShare', description: 'Called with selected username' },
            onClose: { action: 'onClose', description: 'Called when modal closes' },
        }
    });
</script>

<script lang="ts">
    const mockUsers = [
        { user_id: 1, username: 'alice' },
        { user_id: 2, username: 'bob' },
        { user_id: 3, username: 'carol' },
    ];

    const manyUsers = Array.from({ length: 12 }, (_, i) => ({
        user_id: i + 1,
        username: `user_${String.fromCharCode(97 + i)}`,
    }));

    function handleShare(username: string) {
        console.log('Story: Share with', username);
    }

    function handleClose() {
        console.log('Story: Modal closed');
    }
</script>

<Story
    name="Default"
    args={{
        isOpen: true,
        users: mockUsers,
        fileName: 'document.pdf',
        onShare: handleShare,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <ShareFileModal {...args} />
    {/snippet}
</Story>

<Story
    name="Empty Users"
    args={{
        isOpen: true,
        users: [],
        fileName: 'photo.jpg',
        onShare: handleShare,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <ShareFileModal {...args} />
    {/snippet}
</Story>

<Story
    name="Loading"
    args={{
        isOpen: true,
        users: mockUsers,
        fileName: 'report.xlsx',
        loading: true,
        onShare: handleShare,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <ShareFileModal {...args} />
    {/snippet}
</Story>

<Story
    name="Error"
    args={{
        isOpen: true,
        users: mockUsers,
        fileName: 'notes.txt',
        error: 'Already shared with this user',
        onShare: handleShare,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <ShareFileModal {...args} />
    {/snippet}
</Story>

<Story
    name="Success"
    args={{
        isOpen: true,
        users: mockUsers,
        fileName: 'budget.csv',
        success: 'File shared successfully!',
        onShare: handleShare,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <ShareFileModal {...args} />
    {/snippet}
</Story>

<Story
    name="Many Users"
    args={{
        isOpen: true,
        users: manyUsers,
        fileName: 'presentation.pptx',
        onShare: handleShare,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <ShareFileModal {...args} />
    {/snippet}
</Story>
