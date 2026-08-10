<script lang="ts">
    import type { Snippet } from 'svelte'

    /**
     * The grouping surface: one element owning background, border and radius,
     * so the corners can't disagree the way the split wrapper/panel chrome in
     * the resilience pane does. bg-surface0 on the app background is what
     * gives the group its contrast; the border is the subtle one (overlay0)
     * because it no longer has to carry that job alone.
     */
    let {
        title,
        subtitle,
        icon,
        headerRight,
        padding = true,
        children,
        className = ''
    }: {
        /** Header title; without it no header row is rendered. */
        title?: string,
        /** Muted line under the title. */
        subtitle?: string,
        /** Carbon icon class rendered left of the title. */
        icon?: string,
        /** Right side of the header row — a status span, small actions. */
        headerRight?: Snippet,
        /** false for full-bleed content (charts, tables). */
        padding?: boolean,
        /** Optional: a status card can be its header alone. */
        children?: Snippet,
        /** Extra classes on the card element (opacity, width constraints). */
        className?: string
    } = $props()
</script>

<div class="bg-surface0 border border-overlay0 rounded-lg overflow-hidden {padding ? 'p-4' : ''} {className}">
    {#if title}
        <div class="flex items-baseline justify-between gap-3 {children ? 'mb-4' : ''} {padding ? '' : 'p-4 pb-0'}">
            <div class="flex-1 min-w-0">
                <h4 class="flex items-center gap-2 text-lg font-semibold text-primary">
                    {#if icon}
                        <span class="{icon} flex-shrink-0" aria-hidden="true"></span>
                    {/if}
                    <span class="truncate">{title}</span>
                </h4>
                {#if subtitle}
                    <p class="text-sm text-muted mt-1">{subtitle}</p>
                {/if}
            </div>
            {#if headerRight}
                {@render headerRight()}
            {/if}
        </div>
    {/if}
    {#if children}
        {@render children()}
    {/if}
</div>
