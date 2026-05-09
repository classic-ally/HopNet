<script lang="ts">
    import type { Snippet } from 'svelte';

    interface BannerProps {
        title: string;
        subtitle?: string;
        icon?: string;        // Iconify class, e.g. "i-carbon-information"
        variant?: 'info' | 'warning' | 'success';
        onClick?: () => void; // optional click handler — banner becomes interactive

        // Snippet for trailing content (button, progress bar, etc.)
        action?: Snippet;
    }

    let {
        title,
        subtitle = undefined,
        icon = undefined,
        variant = 'info',
        onClick = undefined,
        action,
    }: BannerProps = $props();

    const variantClasses: Record<string, string> = {
        info:    'bg-blue/20 border-blue text-blue',
        warning: 'bg-yellow/20 border-yellow text-yellow',
        success: 'bg-green/20 border-green text-green',
    };

    const defaultIcons: Record<string, string> = {
        info:    'i-carbon-information',
        warning: 'i-carbon-warning',
        success: 'i-carbon-checkmark',
    };

    const resolvedIcon = $derived(icon ?? defaultIcons[variant]);
    const interactive = $derived(typeof onClick === 'function');
</script>

<div
    class="sticky top-0 z-30 flex items-center gap-3 px-4 py-2 border-b {variantClasses[variant]} {interactive ? 'cursor-pointer hover:opacity-80' : ''}"
    role={interactive ? 'button' : undefined}
    tabindex={interactive ? 0 : undefined}
    onclick={onClick}
    onkeydown={(e) => { if (interactive && (e.key === 'Enter' || e.key === ' ')) onClick?.(); }}
>
    <div class="{resolvedIcon} flex-shrink-0 text-lg"></div>
    <div class="flex-1 min-w-0">
        <div class="text-sm font-medium truncate">{title}</div>
        {#if subtitle}<div class="text-xs opacity-80 truncate">{subtitle}</div>{/if}
    </div>
    {#if action}
        <div class="flex-shrink-0">{@render action()}</div>
    {/if}
</div>
