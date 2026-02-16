<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import IncomingSharesList from './IncomingSharesList.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Shares/IncomingSharesList',
        component: IncomingSharesList,
        argTypes: {
            onAccept: { action: 'onAccept', description: 'Called when Accept is clicked' },
            onDecline: { action: 'onDecline', description: 'Called when Decline is clicked' },
        }
    });
</script>

<script lang="ts">
    const mockShares = [
        { id: '1', sender_username: 'alice', display_name: 'project-plan.pdf', created_at: '2026-02-15T10:30:00Z' },
        { id: '2', sender_username: 'bob', display_name: 'budget-2026.xlsx', created_at: '2026-02-14T08:15:00Z' },
        { id: '3', sender_username: 'carol', display_name: 'meeting-notes.txt', created_at: '2026-02-13T16:45:00Z' },
    ];

    function handleAccept(share: any) {
        console.log('Story: Accept', share.display_name);
    }

    function handleDecline(share: any) {
        console.log('Story: Decline', share.display_name);
    }
</script>

<Story
    name="With Shares"
    args={{
        shares: mockShares,
        onAccept: handleAccept,
        onDecline: handleDecline,
    }}
>
    {#snippet template(args)}
        <div class="max-w-xl">
            <IncomingSharesList {...args} />
        </div>
    {/snippet}
</Story>

<Story
    name="Empty"
    args={{
        shares: [],
        onAccept: handleAccept,
        onDecline: handleDecline,
    }}
>
    {#snippet template(args)}
        <div class="max-w-xl">
            <IncomingSharesList {...args} />
        </div>
    {/snippet}
</Story>

<Story
    name="Loading"
    args={{
        loading: true,
        onAccept: handleAccept,
        onDecline: handleDecline,
    }}
>
    {#snippet template(args)}
        <div class="max-w-xl">
            <IncomingSharesList {...args} />
        </div>
    {/snippet}
</Story>
