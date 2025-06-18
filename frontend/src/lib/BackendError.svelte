<script lang="ts">
    import SetupPane from './SetupPane.svelte';
</script>

<SetupPane
    title="Could not connect to backend."
    body="Try restarting the app."
>
    {#snippet features()}

    {/snippet}

    {#snippet buttons()}

    {/snippet}
</SetupPane>