<script lang="ts">
    import Button from "../../Button.svelte";
    import SetupPane from "../../SetupPane.svelte";
    import PassphraseDisplayContent from "../../primitives/PassphraseDisplayContent.svelte";

    export let passphrase: string;
    export let onContinue: () => void;
</script>

<SetupPane
    title="Your Passphrase"
    body="Write this down and store it somewhere safe. This passphrase cannot be recovered if lost."
>
    {#snippet features()}
        <PassphraseDisplayContent {passphrase} />
    {/snippet}

    {#snippet buttons()}
        <div></div>
        <Button
            icon="i-carbon-checkmark"
            text="I've written it down"
            onClick={onContinue}
            position="right"
        />
    {/snippet}
</SetupPane>
