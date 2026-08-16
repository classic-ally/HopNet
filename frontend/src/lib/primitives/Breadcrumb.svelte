<script lang="ts">
    import DropdownMenu from './DropdownMenu.svelte'

    /**
     * A path trail. Data-driven rather than shadcn's compositional parts,
     * because every caller here has a path rather than arbitrary markup, and
     * the collapse logic — the only hard part — belongs in one place.
     *
     * Omitting `onNavigate` gives a read-only path display, which is the mode
     * the upload and new-folder modals use. When they later become
     * destination pickers, only the callback changes.
     */

    export interface Crumb {
        label: string
        /** Handed back to onNavigate — the folder path, here. */
        value: string
        /** When set, renders <a href> so middle-click and new-tab work. */
        href?: string
        icon?: string
        /** Icon replaces the label entirely (the root crumb). */
        iconOnly?: boolean
    }

    let {
        segments,
        onNavigate,
        maxVisible = 4,
        ariaLabel = 'Breadcrumb',
        className = ''
    }: {
        segments: Crumb[],
        /** Omit for a read-only display — nothing becomes clickable. */
        onNavigate?: (value: string) => void,
        /** Crumbs shown before the middle collapses behind an ellipsis. */
        maxVisible?: number,
        ariaLabel?: string,
        className?: string
    } = $props()

    const interactive = $derived(Boolean(onNavigate))
    const collapsed = $derived(segments.length > maxVisible)

    // Keep the first crumb (usually root) and the deepest ones; the middle
    // collapses, so depth can never shove the row.
    const leading = $derived(collapsed ? segments.slice(0, 1) : segments)
    const hidden = $derived(collapsed ? segments.slice(1, segments.length - (maxVisible - 2)) : [])
    const trailing = $derived(collapsed ? segments.slice(segments.length - (maxVisible - 2)) : [])

    const hiddenLabel = $derived(
        `Show ${hidden.length} hidden ${hidden.length === 1 ? 'folder' : 'folders'}`
    )

    const hiddenItems = $derived(
        hidden.map((crumb) => ({
            label: crumb.label,
            href: crumb.href,
            icon: 'i-carbon-folder',
            onSelect: () => onNavigate?.(crumb.value)
        }))
    )

    /**
     * Modified clicks belong to the browser: cmd/ctrl-click must open a new
     * tab rather than being swallowed, which is the whole point of having an
     * href on a crumb.
     */
    function onCrumbClick(event: MouseEvent, crumb: Crumb) {
        if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return
        event.preventDefault()
        onNavigate?.(crumb.value)
    }

    const LINK = 'text-blue hover:text-primary hover:underline cursor-pointer bg-transparent border-none p-0 truncate ' +
        'focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-mauve rounded'
</script>

<nav aria-label={ariaLabel} class="min-w-0 {className}">
    <ol class="flex items-center gap-1 min-w-0 overflow-hidden text-sm font-mono list-none m-0 p-0">
        {#each leading as crumb, i (crumb.value)}
            {@const isLeaf = !collapsed && i === segments.length - 1}
            <li class="flex items-center gap-1 min-w-0">
                {#if i > 0}
                    <span role="presentation" aria-hidden="true" class="text-overlay1">/</span>
                {/if}
                {#if isLeaf}
                    <span aria-current="page" class="text-primary truncate min-w-0">
                        {#if crumb.icon}<span class="{crumb.icon} w-4 h-4 inline-block align-middle" aria-hidden="true"></span>{/if}
                        {#if !crumb.iconOnly}{crumb.label}{/if}
                    </span>
                {:else if interactive}
                    <svelte:element
                        this={crumb.href ? 'a' : 'button'}
                        href={crumb.href}
                        type={crumb.href ? undefined : 'button'}
                        class={LINK}
                        aria-label={crumb.iconOnly ? crumb.label : undefined}
                        onclick={(event: MouseEvent) => onCrumbClick(event, crumb)}
                    >
                        {#if crumb.icon}<span class="{crumb.icon} w-4 h-4 inline-block align-middle" aria-hidden="true"></span>{/if}
                        {#if !crumb.iconOnly}{crumb.label}{/if}
                    </svelte:element>
                {:else}
                    <span class="text-muted truncate">
                        {#if crumb.icon}<span class="{crumb.icon} w-4 h-4 inline-block align-middle" aria-hidden="true"></span>{/if}
                        {#if !crumb.iconOnly}{crumb.label}{/if}
                    </span>
                {/if}
            </li>
        {/each}

        {#if collapsed}
            <li class="flex items-center gap-1">
                <span role="presentation" aria-hidden="true" class="text-overlay1">/</span>
                {#if interactive}
                    <DropdownMenu items={hiddenItems} ariaLabel={hiddenLabel} trigger={ellipsisTrigger} />
                {:else}
                    <!-- Read-only: the hidden trail lives in the tooltip, the
                         same "value truncated, full string in title" pattern
                         the takeout id column uses. -->
                    <span class="text-muted" title={hidden.map((c) => c.label).join(' / ')}>…</span>
                {/if}
            </li>
        {/if}

        {#each trailing as crumb, i (crumb.value)}
            {@const isLeaf = i === trailing.length - 1}
            <li class="flex items-center gap-1 min-w-0">
                <span role="presentation" aria-hidden="true" class="text-overlay1">/</span>
                {#if isLeaf}
                    <span aria-current="page" class="text-primary truncate min-w-0">{crumb.label}</span>
                {:else if interactive}
                    <svelte:element
                        this={crumb.href ? 'a' : 'button'}
                        href={crumb.href}
                        type={crumb.href ? undefined : 'button'}
                        class={LINK}
                        onclick={(event: MouseEvent) => onCrumbClick(event, crumb)}
                    >
                        {crumb.label}
                    </svelte:element>
                {:else}
                    <span class="text-muted truncate">{crumb.label}</span>
                {/if}
            </li>
        {/each}
    </ol>
</nav>

{#snippet ellipsisTrigger(ctx: { props: Record<string, unknown> })}
    <!-- The icon is aria-hidden, so the button carries the accessible name. -->
    <button
        type="button"
        aria-label={hiddenLabel}
        class="text-muted hover:text-primary bg-transparent border-none cursor-pointer px-1 rounded
               focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-mauve"
        {...ctx.props}
    >
        <span class="i-carbon-overflow-menu-horizontal w-4 h-4 block" aria-hidden="true"></span>
    </button>
{/snippet}
