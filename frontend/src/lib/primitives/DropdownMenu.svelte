<script lang="ts">
    import type { Snippet } from 'svelte'
    import { portal } from './portal'

    /**
     * A menu anchored to a trigger. The panel is portalled to <body> and
     * positioned with fixed coordinates from the trigger's rect, because an
     * absolutely positioned panel is clipped by Card's overflow-hidden — which
     * is exactly where the breadcrumb's collapse menu lives.
     *
     * Items are data, deliberately with no arbitrary-content escape hatch:
     * non-menuitem content inside role="menu" is invalid, and that is the bug
     * the photos FilterDropdown has today.
     */

    export interface MenuItem {
        label: string
        icon?: string
        /** Renders <a role="menuitem">, so middle-click and new-tab work. */
        href?: string
        onSelect?: () => void
        disabled?: boolean
        /** Red tone, for destructive actions. */
        destructive?: boolean
        /** Draws a separator above this item. */
        separatorBefore?: boolean
    }

    let {
        items,
        trigger,
        align = 'start',
        open = $bindable(false),
        ariaLabel,
        className = ''
    }: {
        items: MenuItem[],
        /** The opening control. Spread `props` onto your button. */
        trigger: Snippet<[{ props: Record<string, unknown>, open: boolean }]>,
        align?: 'start' | 'end',
        open?: boolean,
        ariaLabel?: string,
        className?: string
    } = $props()

    const panelId = $props.id()

    let anchorEl: HTMLSpanElement | undefined = $state()
    let panelEl: HTMLDivElement | undefined = $state()
    let position = $state({ top: 0, left: 0 })

    const enabledIndices = $derived(
        items.flatMap((item, i) => (item.disabled ? [] : [i]))
    )

    /** The trigger's own focusable element, for focus restore on close. */
    function triggerEl(): HTMLElement | null {
        return anchorEl?.querySelector('button, a, [tabindex]') ?? null
    }

    function place() {
        if (!anchorEl) return
        const rect = anchorEl.getBoundingClientRect()
        const panelWidth = panelEl?.offsetWidth ?? 224
        const panelHeight = panelEl?.offsetHeight ?? 0

        let top = rect.bottom + 6
        // Flip above when the panel would run off the bottom.
        if (panelHeight > 0 && top + panelHeight > window.innerHeight - 8) {
            top = Math.max(8, rect.top - panelHeight - 6)
        }

        let left = align === 'end' ? rect.right - panelWidth : rect.left
        left = Math.max(8, Math.min(left, window.innerWidth - panelWidth - 8))

        position = { top, left }
    }

    function focusItem(index: number) {
        panelEl?.querySelectorAll<HTMLElement>('[role="menuitem"]')[index]?.focus()
    }

    /** Index within `items` of the currently focused menu item, or -1. */
    function focusedIndex(): number {
        const nodes = [...(panelEl?.querySelectorAll<HTMLElement>('[role="menuitem"]') ?? [])]
        return nodes.indexOf(document.activeElement as HTMLElement)
    }

    function step(delta: number) {
        if (enabledIndices.length === 0) return
        const current = focusedIndex()
        const at = enabledIndices.indexOf(current)
        // Wrap; an unfocused panel enters at the first (or last) enabled item.
        const next =
            at === -1
                ? delta > 0
                    ? enabledIndices[0]
                    : enabledIndices[enabledIndices.length - 1]
                : enabledIndices[(at + delta + enabledIndices.length) % enabledIndices.length]
        focusItem(next)
    }

    function close(restoreFocus = true) {
        open = false
        if (restoreFocus) triggerEl()?.focus()
    }

    function select(item: MenuItem) {
        if (item.disabled) return
        // href items navigate on their own; the menu just gets out of the way.
        if (!item.href) item.onSelect?.()
        close()
    }

    function onPanelKeydown(event: KeyboardEvent) {
        switch (event.key) {
            case 'ArrowDown':
                event.preventDefault()
                step(1)
                break
            case 'ArrowUp':
                event.preventDefault()
                step(-1)
                break
            case 'Home':
                event.preventDefault()
                focusItem(enabledIndices[0])
                break
            case 'End':
                event.preventDefault()
                focusItem(enabledIndices[enabledIndices.length - 1])
                break
            case 'Escape':
                event.preventDefault()
                event.stopPropagation()
                close()
                break
            case 'Tab':
                // Menus are not focus traps: let Tab move on, but close first.
                close(false)
                break
        }
    }

    const triggerProps = $derived({
        'aria-haspopup': 'menu' as const,
        'aria-expanded': open,
        'aria-controls': panelId,
        onclick: () => (open = !open),
        onkeydown: (event: KeyboardEvent) => {
            if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
                event.preventDefault()
                open = true
            }
        }
    })

    // Position on open, then follow the trigger. Scroll is captured so nested
    // scrollers count; the panel follows rather than closing, since a menu
    // detaching from its trigger reads as a glitch.
    $effect(() => {
        if (!open) return
        place()
        // The first enabled item takes focus, so arrow keys work immediately.
        const raf = requestAnimationFrame(() => {
            place()
            if (enabledIndices.length > 0) focusItem(enabledIndices[0])
        })
        const onScroll = () => place()
        window.addEventListener('scroll', onScroll, { capture: true, passive: true })
        window.addEventListener('resize', onScroll)
        return () => {
            cancelAnimationFrame(raf)
            window.removeEventListener('scroll', onScroll, { capture: true })
            window.removeEventListener('resize', onScroll)
        }
    })

    function onDocumentPointerDown(event: PointerEvent) {
        if (!open) return
        const target = event.target as Node
        if (anchorEl?.contains(target) || panelEl?.contains(target)) return
        close(false)
    }
</script>

<svelte:document onpointerdown={onDocumentPointerDown} />

<span class="inline-flex" bind:this={anchorEl}>
    {@render trigger({ props: triggerProps, open })}
</span>

{#if open}
    <div
        use:portal
        bind:this={panelEl}
        id={panelId}
        role="menu"
        aria-label={ariaLabel}
        tabindex="-1"
        onkeydown={onPanelKeydown}
        class="fixed z-60 min-w-56 max-h-80 overflow-y-auto bg-mantle border border-overlay0 rounded-md py-1 shadow-lg {className}"
        style="top: {position.top}px; left: {position.left}px"
    >
        {#each items as item, i (item.label + i)}
            {#if item.separatorBefore && i > 0}
                <div role="separator" class="my-1 border-t border-overlay0"></div>
            {/if}
            {#if item.href && !item.disabled}
                <a
                    role="menuitem"
                    tabindex="-1"
                    href={item.href}
                    class="flex items-center gap-2 px-3 py-1.5 text-sm no-underline
                           {item.destructive ? 'text-red' : 'text-primary'}
                           hover:bg-surface0 focus-visible:bg-surface0 focus-visible:outline-none"
                    onclick={() => select(item)}
                >
                    {#if item.icon}<span class="{item.icon} w-4 h-4 flex-shrink-0" aria-hidden="true"></span>{/if}
                    <span class="truncate">{item.label}</span>
                </a>
            {:else}
                <button
                    type="button"
                    role="menuitem"
                    tabindex="-1"
                    disabled={item.disabled}
                    aria-disabled={item.disabled}
                    class="w-full flex items-center gap-2 px-3 py-1.5 text-sm text-left bg-transparent border-none
                           {item.destructive ? 'text-red' : 'text-primary'}
                           {item.disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer hover:bg-surface0'}
                           focus-visible:bg-surface0 focus-visible:outline-none"
                    onclick={() => select(item)}
                >
                    {#if item.icon}<span class="{item.icon} w-4 h-4 flex-shrink-0" aria-hidden="true"></span>{/if}
                    <span class="truncate">{item.label}</span>
                </button>
            {/if}
        {/each}
    </div>
{/if}
