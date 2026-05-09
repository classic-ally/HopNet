<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import OnboardingChecklistItem from './OnboardingChecklistItem.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Onboarding/OnboardingChecklistItem',
        component: OnboardingChecklistItem,
        argTypes: {
            status: { control: 'radio', options: ['todo', 'active', 'done'] },
            icon: { control: 'text' },
            title: { control: 'text' },
            summary: { control: 'text' },
        },
    });
</script>

<Story name="Todo" args={{
    icon: 'i-carbon-cloud-upload',
    title: 'Import existing data',
    summary: 'Bring in a HopNet takeout archive from a previous installation',
    status: 'todo',
    onClick: () => console.log('clicked'),
}}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <OnboardingChecklistItem {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Active (in progress)" args={{
    icon: 'i-carbon-cloud-upload',
    title: 'Import existing data',
    summary: 'Bring in a HopNet takeout archive from a previous installation',
    status: 'active',
    onClick: () => console.log('clicked'),
}}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <OnboardingChecklistItem {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Done" args={{
    icon: 'i-carbon-cloud-upload',
    title: 'Import existing data',
    summary: 'Bring in a HopNet takeout archive from a previous installation',
    status: 'done',
    onClick: () => console.log('clicked'),
}}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <OnboardingChecklistItem {...args} />
        </div>
    {/snippet}
</Story>
