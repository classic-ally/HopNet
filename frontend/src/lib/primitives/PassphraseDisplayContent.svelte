<script lang="ts">
    interface Props {
        passphrase: string;
    }

    let { passphrase }: Props = $props();

    let words = $derived(passphrase.split(' '));

    let copied = $state(false);
    async function copyPassphrase() {
        await navigator.clipboard.writeText(passphrase);
        copied = true;
        setTimeout(() => { copied = false; }, 2000);
    }
</script>

<div class="bg-mantle rounded-lg p-4 font-mono text-sm relative">
    <button
        class="absolute top-2 right-2 text-overlay1 hover:text-text transition-colors cursor-pointer bg-transparent border-none p-1"
        title={copied ? 'Copied!' : 'Copy passphrase'}
        aria-label={copied ? 'Copied!' : 'Copy passphrase'}
        onclick={copyPassphrase}
    >
        <div class={copied ? 'i-carbon-checkmark text-green' : 'i-carbon-copy'} ></div>
    </button>
    {#each words as word, i}
        <div class="flex gap-3 py-1">
            <span class="text-overlay0 w-5 text-right">{i + 1}.</span>
            <span class="text-text">{word}</span>
        </div>
    {/each}
</div>
<p class="text-red text-xs mt-1">This is the only time your passphrase will be shown.</p>
