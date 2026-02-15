<script lang="ts">
    import Button from "../../Button.svelte";
    import EntryRow from "../../EntryRow.svelte";
    import SetupPane from "../../SetupPane.svelte";

    export let passphrase: string;
    export let onVerified: () => void;

    let errorMessage = '';

    // Pick 3 random distinct positions using Fisher-Yates partial shuffle
    const words = passphrase.split(' ');
    const indices = Array.from({ length: words.length }, (_, i) => i);
    for (let i = indices.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [indices[i], indices[j]] = [indices[j], indices[i]];
    }
    const challengePositions = indices.slice(0, 3).sort((a, b) => a - b);

    let answers: string[] = ['', '', ''];

    function verify() {
        errorMessage = '';
        for (let i = 0; i < 3; i++) {
            const expected = words[challengePositions[i]].toLowerCase().trim();
            const actual = answers[i].toLowerCase().trim();
            if (actual !== expected) {
                errorMessage = 'One or more words are incorrect. Please try again.';
                return;
            }
        }
        onVerified();
    }
</script>

<SetupPane
    title="Verify Passphrase"
    body="Enter the following words from your passphrase to confirm you've written it down."
>
    {#snippet features()}
        {#each challengePositions as pos, i}
            <EntryRow
                icon="i-carbon-text-short-paragraph"
                title="Word #{pos + 1}"
                password={false}
                bind:value={answers[i]}
            />
        {/each}
        {#if errorMessage}
            <p class="text-red text-sm mt-2">{errorMessage}</p>
        {/if}
    {/snippet}

    {#snippet buttons()}
        <div></div>
        <Button
            icon="i-carbon-checkmark"
            text="Verify"
            onClick={verify}
            position="right"
        />
    {/snippet}
</SetupPane>
