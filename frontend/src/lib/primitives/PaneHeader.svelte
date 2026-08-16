<script lang="ts">
    /**
     * A pane's title and its one-line description.
     *
     * Extracted because nine panes had grown the same shape independently — a
     * bare `<h3>` over a `text-sm text-muted` paragraph. The title relied on the
     * browser's default heading size rather than naming one, which meant it
     * silently flattened to body text the moment a reset established
     * `font-size: inherit` on headings. Naming the size here fixes all nine at
     * once and gives the next pane something to reach for.
     *
     * `text-lg font-semibold` is not a new choice — it is what Modal, FilePreview,
     * MaintenancePane's cards, ConsensusPanel and StoragePanel already use.
     */
    interface PaneHeaderProps {
        title: string;
        /// Omitted by panes with nothing useful to say under the title.
        subtitle?: string;
    }

    let { title, subtitle = undefined }: PaneHeaderProps = $props();
</script>

<div>
    <h3 class="text-lg font-semibold">{title}</h3>
    {#if subtitle}
        <p class="text-sm text-muted">{subtitle}</p>
    {/if}
</div>
