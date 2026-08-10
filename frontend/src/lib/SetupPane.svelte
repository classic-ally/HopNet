<script lang="ts">
    import type { Snippet } from 'svelte';
    import Modal from './primitives/Modal.svelte';

    /**
     * The setup flow's card, now a thin wrapper over Modal rather than a second
     * card chrome maintained alongside it.
     *
     * Setup is a modal that cannot be dismissed: all three of Modal's exits are
     * closed here, once, instead of in each of the nine callers. There is no
     * meaningful cancel — the flow either completes or the app has nothing to
     * show. Note this is a convention, not an enforcement: Modal has no focus
     * trap, so Tab still leaves the panel. Harmless during first-run setup,
     * where nothing is rendered behind it.
     *
     * The public shape is unchanged, so no caller needed touching.
     */
    interface SetupPaneProps {
        title: string;
        body?: string;
        logoSrc?: string;
        /**
         * Layout for the button row. Still a class string because two callers
         * depend on specific arrangements and Modal's footer holds no layout
         * opinion of its own.
         */
        buttonsClass?: string;
        features: Snippet;
        /// Omitted by steps that have no actions of their own (JoinQR waits on a
        /// peer), which keeps Modal from drawing an empty footer rule.
        buttons?: Snippet;
    }

    let {
        title,
        body = '',
        logoSrc = undefined,
        buttonsClass = 'grid grid-cols-2 gap-2',
        features,
        buttons = undefined,
    }: SetupPaneProps = $props();
</script>

{#snippet footerRow()}
    <div class={buttonsClass}>
        {@render buttons?.()}
    </div>
{/snippet}

<Modal
    {title}
    showCloseButton={false}
    closeOnBackdrop={false}
    closeOnEscape={false}
    footer={buttons ? footerRow : undefined}
>
    {#snippet content()}
        {#if logoSrc}
            <img src={logoSrc} alt="HopNet" class="w-40 h-auto mx-auto block mt-2" />
        {/if}
        {#if body}
            <p>{body}</p>
        {/if}
        <div class="flex flex-col gap-2">
            {@render features()}
        </div>
    {/snippet}
</Modal>
