<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import ShareDetailsModal from './ShareDetailsModal.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Files/ShareDetailsModal',
        component: ShareDetailsModal,
        argTypes: {
            isOpen: { control: 'boolean', description: 'Whether the modal is open' },
            onUnshare: { action: 'onUnshare', description: 'Called when Leave Share is clicked' },
            onClose: { action: 'onClose', description: 'Called when modal closes' },
        }
    });
</script>

<script lang="ts">
    function handleUnshare() {
        console.log('Story: Leave share');
    }

    function handleClose() {
        console.log('Story: Modal closed');
    }
</script>

<Story
    name="Multiple Participants"
    args={{
        isOpen: true,
        fileName: 'project-plan.pdf',
        currentUserId: 1,
        participants: [
            { username: 'alice', user_id: 1, status: 'accepted' },
            { username: 'bob', user_id: 2, status: 'accepted' },
            { username: 'carol', user_id: 3, status: 'accepted' },
        ],
        onUnshare: handleUnshare,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <ShareDetailsModal {...args} />
    {/snippet}
</Story>

<Story
    name="Solo"
    args={{
        isOpen: true,
        fileName: 'notes.txt',
        currentUserId: 1,
        participants: [
            { username: 'alice', user_id: 1, status: 'accepted' },
        ],
        onUnshare: handleUnshare,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <ShareDetailsModal {...args} />
    {/snippet}
</Story>

<Story
    name="With Pending"
    args={{
        isOpen: true,
        fileName: 'budget-2026.xlsx',
        currentUserId: 1,
        participants: [
            { username: 'alice', user_id: 1, status: 'accepted' },
            { username: 'bob', user_id: 2, status: 'pending' },
            { username: 'carol', user_id: 3, status: 'accepted' },
        ],
        onUnshare: handleUnshare,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <ShareDetailsModal {...args} />
    {/snippet}
</Story>

<Story
    name="Loading"
    args={{
        isOpen: true,
        fileName: 'document.pdf',
        currentUserId: 1,
        participants: [],
        loading: true,
        onUnshare: handleUnshare,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <ShareDetailsModal {...args} />
    {/snippet}
</Story>
