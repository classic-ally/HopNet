<script lang="ts">
    import SetupPane from "../../SetupPane.svelte";
    import { liveSetupApi, TRANSPORT_FAILURE, type SetupApi } from '../../api/setup';

    export let api: SetupApi = liveSetupApi;
    /// Returns to the create-or-join choice, for a misclick.
    export let onBackButton: () => void;
    /// Advances to the pairing QR once the code is adopted.
    export let onCodeAccepted: () => void;

    let raw = '';
    let submitting = false;
    let error = '';

    /**
     * Display normalization only — the server's parser is equally
     * tolerant (case, dashes, whitespace), so nothing is lost if the
     * user pastes an unformatted code and submits before the input
     * re-renders.
     */
    function formatted(value: string): string {
        const digits = value.replace(/[^0-9a-fA-F]/g, '').toUpperCase().slice(0, 8);
        return digits.length > 4 ? `${digits.slice(0, 4)}-${digits.slice(4)}` : digits;
    }

    $: raw = formatted(raw);
    $: complete = raw.replace('-', '').length === 8;

    async function submit() {
        if (!complete || submitting) return;
        submitting = true;
        error = '';
        const result = await api.submitJoinCode(raw);
        submitting = false;
        if (result.ok) {
            onCodeAccepted();
        } else if (result.status === TRANSPORT_FAILURE) {
            error = `Network error: ${result.detail ?? 'Unknown error'}`;
        } else if (result.status === 409) {
            error = result.detail ??
                'A different code was already entered — restart this device to re-enter.';
        } else {
            error = result.detail ?? 'That is not a valid mesh code.';
        }
    }
</script>

<SetupPane
    title="Enter the mesh code"
    body="Open the Nodes page on a device already in the network and click Add Node — the code shown there pairs this device to that network."
    onBack={onBackButton}
>
    {#snippet features()}
        <form class="flex flex-col gap-4" on:submit|preventDefault={submit}>
            <input
                class="rounded border border-overlay0 bg-transparent px-4 py-3 text-center
                       font-mono text-2xl tracking-widest uppercase"
                placeholder="XXXX-XXXX"
                autocomplete="off"
                spellcheck="false"
                bind:value={raw}
                aria-label="Mesh code"
            />
            {#if error}
                <p class="text-red text-sm" role="alert">{error}</p>
            {/if}
            <button
                type="submit"
                class="rounded bg-mauve px-4 py-2 font-semibold text-base disabled:opacity-50"
                disabled={!complete || submitting}
            >
                {submitting ? 'Connecting…' : 'Continue'}
            </button>
        </form>
    {/snippet}
</SetupPane>
