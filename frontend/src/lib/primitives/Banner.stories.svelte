<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import Banner from './Banner.svelte';

    const { Story } = defineMeta({
        title: 'Primitives/Banner',
        component: Banner,
        argTypes: {
            variant: {
                control: 'radio',
                options: ['info', 'warning', 'success'],
            },
            title: { control: 'text' },
            subtitle: { control: 'text' },
            icon: { control: 'text' },
        },
    });
</script>

<Story name="Info — title only" args={{ variant: 'info', title: 'Heads up: something is happening.' }}>
    {#snippet template(args)}
        <Banner {...args} />
    {/snippet}
</Story>

<Story name="Info — with subtitle" args={{
    variant: 'info',
    title: 'Import in progress',
    subtitle: '3 of 15 files processed. Uploads are paused until it finishes.',
}}>
    {#snippet template(args)}
        <Banner {...args} />
    {/snippet}
</Story>

<Story name="Info — interactive (click to navigate)" args={{
    variant: 'info',
    title: 'Import in progress',
    subtitle: 'Click to view progress.',
    onClick: () => console.log('Banner clicked'),
}}>
    {#snippet template(args)}
        <Banner {...args} />
    {/snippet}
</Story>

<Story name="Warning" args={{
    variant: 'warning',
    title: 'Quota almost full',
    subtitle: 'Free up space or your next upload may fail.',
}}>
    {#snippet template(args)}
        <Banner {...args} />
    {/snippet}
</Story>

<Story name="Success" args={{
    variant: 'success',
    title: 'Import complete',
    subtitle: '15 of 15 files imported successfully.',
}}>
    {#snippet template(args)}
        <Banner {...args} />
    {/snippet}
</Story>
