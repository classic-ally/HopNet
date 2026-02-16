<script lang="ts">
    import Button from "../../Button.svelte";
    import SetupPane from "../../SetupPane.svelte";
    import PassphraseVerifyContent from "../../primitives/PassphraseVerifyContent.svelte";

    export let passphrase: string;
    export let onVerified: () => void;

    let verifyRef: PassphraseVerifyContent;

    function handleVerify() {
        if (verifyRef.verify()) {
            onVerified();
        }
    }
</script>

<SetupPane
    title="Verify Passphrase"
    body="Enter the following words from your passphrase to confirm you've written it down."
>
    {#snippet features()}
        <PassphraseVerifyContent {passphrase} bind:this={verifyRef} />
    {/snippet}

    {#snippet buttons()}
        <div></div>
        <Button
            icon="i-carbon-checkmark"
            text="Verify"
            onClick={handleVerify}
            position="right"
        />
    {/snippet}
</SetupPane>
