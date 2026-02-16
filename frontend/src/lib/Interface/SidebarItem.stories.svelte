<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import SidebarItem from './SidebarItem.svelte';

    const { Story } = defineMeta({
        title: 'Interface/SidebarItem',
        component: SidebarItem,
        argTypes: {
            onClick: { action: 'onClick', description: 'Called when item is clicked' },
        }
    });
</script>

<script lang="ts">
    function handleClick() {
        console.log('Story: SidebarItem clicked');
    }
</script>

<Story
    name="Default"
    args={{
        icon: 'i-carbon-folder',
        title: 'Browse',
        selected: false,
        onClick: handleClick,
    }}
>
    {#snippet template(args)}
        <div class="w-56 bg-mantle p-4">
            <SidebarItem {...args} />
        </div>
    {/snippet}
</Story>

<Story
    name="Selected"
    args={{
        icon: 'i-carbon-folder',
        title: 'Browse',
        selected: true,
        onClick: handleClick,
    }}
>
    {#snippet template(args)}
        <div class="w-56 bg-mantle p-4">
            <SidebarItem {...args} />
        </div>
    {/snippet}
</Story>

<Story
    name="With Badge"
    args={{
        icon: 'i-carbon-collaborate',
        title: 'Shared With Me',
        selected: false,
        badge: 3,
        onClick: handleClick,
    }}
>
    {#snippet template(args)}
        <div class="w-56 bg-mantle p-4">
            <SidebarItem {...args} />
        </div>
    {/snippet}
</Story>

<Story
    name="With Badge Selected"
    args={{
        icon: 'i-carbon-collaborate',
        title: 'Shared With Me',
        selected: true,
        badge: 3,
        onClick: handleClick,
    }}
>
    {#snippet template(args)}
        <div class="w-56 bg-mantle p-4">
            <SidebarItem {...args} />
        </div>
    {/snippet}
</Story>

<Story
    name="Large Badge"
    args={{
        icon: 'i-carbon-collaborate',
        title: 'Shared With Me',
        selected: false,
        badge: 42,
        onClick: handleClick,
    }}
>
    {#snippet template(args)}
        <div class="w-56 bg-mantle p-4">
            <SidebarItem {...args} />
        </div>
    {/snippet}
</Story>

<Story
    name="No Badge"
    args={{
        icon: 'i-carbon-collaborate',
        title: 'Shared With Me',
        selected: false,
        badge: 0,
        onClick: handleClick,
    }}
>
    {#snippet template(args)}
        <div class="w-56 bg-mantle p-4">
            <SidebarItem {...args} />
        </div>
    {/snippet}
</Story>
