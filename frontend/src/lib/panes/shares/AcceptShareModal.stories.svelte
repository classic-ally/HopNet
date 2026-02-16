<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import AcceptShareModal from './AcceptShareModal.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Shares/AcceptShareModal',
        component: AcceptShareModal,
        argTypes: {
            isOpen: { control: 'boolean', description: 'Whether the modal is open' },
            onAccept: { action: 'onAccept', description: 'Called with placement path' },
            onClose: { action: 'onClose', description: 'Called when modal closes' },
        }
    });
</script>

<script lang="ts">
    const mockShare = { id: '1', sender_username: 'alice', display_name: 'project-plan.pdf', created_at: '2026-02-15T10:30:00Z' };
    const longNameShare = { id: '2', sender_username: 'bob', display_name: 'quarterly-financial-report-2026-final-version-revised.xlsx', created_at: '2026-02-14T08:15:00Z' };

    function handleAccept(path: string) {
        console.log('Story: Accept to path', path);
    }

    function handleClose() {
        console.log('Story: Modal closed');
    }
</script>

<Story
    name="Default"
    args={{
        isOpen: true,
        share: mockShare,
        onAccept: handleAccept,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <AcceptShareModal {...args} />
    {/snippet}
</Story>

<Story
    name="Loading"
    args={{
        isOpen: true,
        share: mockShare,
        loading: true,
        onAccept: handleAccept,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <AcceptShareModal {...args} />
    {/snippet}
</Story>

<Story
    name="Error"
    args={{
        isOpen: true,
        share: mockShare,
        error: 'A file already exists at this path',
        onAccept: handleAccept,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <AcceptShareModal {...args} />
    {/snippet}
</Story>

<Story
    name="Long Filename"
    args={{
        isOpen: true,
        share: longNameShare,
        onAccept: handleAccept,
        onClose: handleClose,
    }}
>
    {#snippet template(args)}
        <AcceptShareModal {...args} />
    {/snippet}
</Story>
