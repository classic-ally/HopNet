<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import StatusSpinner from './StatusSpinner.svelte';
    import { mergeStatusWords, WHIMSY, GENESIS, AUTH, NETWORK, STORAGE } from './statusWords';

    const { Story } = defineMeta({
        title: 'Primitives/StatusSpinner',
        component: StatusSpinner,
        argTypes: {
            intervalMs: { control: { type: 'number', min: 250, step: 250 } },
            elapsedAfterSecs: { control: { type: 'number', min: 0, step: 1 } },
        },
    });
</script>

<Story name="Setup (BASE + WHIMSY + GENESIS + AUTH)" args={{
    words: mergeStatusWords(WHIMSY, GENESIS, AUTH),
    intervalMs: 2500,
    elapsedAfterSecs: 3,
}}>
    {#snippet template(args)}
        <div class="bg-surface0 p-4 rounded-lg max-w-[400px]">
            <StatusSpinner {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Login (BASE + AUTH)" args={{
    words: mergeStatusWords(AUTH),
    intervalMs: 2500,
    elapsedAfterSecs: 3,
}}>
    {#snippet template(args)}
        <div class="bg-surface0 p-4 rounded-lg max-w-[400px]">
            <StatusSpinner {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Pairing (BASE + WHIMSY + NETWORK)" args={{
    words: mergeStatusWords(WHIMSY, NETWORK),
    intervalMs: 2500,
    elapsedAfterSecs: 3,
}}>
    {#snippet template(args)}
        <div class="bg-surface0 p-4 rounded-lg max-w-[400px]">
            <StatusSpinner {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Storage op (BASE + STORAGE)" args={{
    words: mergeStatusWords(STORAGE),
    intervalMs: 2500,
    elapsedAfterSecs: 3,
}}>
    {#snippet template(args)}
        <div class="bg-surface0 p-4 rounded-lg max-w-[400px]">
            <StatusSpinner {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Fast rotation (500ms) — debug" args={{
    words: mergeStatusWords(WHIMSY, GENESIS, AUTH, NETWORK, STORAGE),
    intervalMs: 500,
    elapsedAfterSecs: 0,
}}>
    {#snippet template(args)}
        <div class="bg-surface0 p-4 rounded-lg max-w-[400px]">
            <StatusSpinner {...args} />
        </div>
    {/snippet}
</Story>
