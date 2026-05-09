<script lang="ts">
    interface ButtonProps {
        icon: string;
        text: string;
        onClick: () => void;
        tooltip?: string;
        variant?: 'desktop' | 'compact' | 'mobile' | 'card';
        position?: 'left' | 'right';
        className?: string;
        disabled?: boolean;

        /// Subtitle text — rendered as second row below `text`. Card variant only.
        subtitle?: string;
        /// Trailing icon class — rendered on the right edge. Card variant only.
        trailing?: string;
        /// Optional class override for the trailing icon (e.g. status colour).
        trailingClass?: string;
        /// Optional secondary text shown next to the trailing icon (e.g. CTA label).
        trailingText?: string;
    }

    let {
        icon,
        text,
        onClick,
        tooltip,
        variant = 'desktop',
        position = 'left',
        className = '',
        disabled = false,
        subtitle = undefined,
        trailing = undefined,
        trailingClass = 'text-subtitle',
        trailingText = undefined,
    }: ButtonProps = $props();

    // Effective tooltip is either the override or the button text
    const effectiveTooltip = $derived(tooltip || text);

    // Size classes based on variant
    const sizeClasses = $derived({
        desktop: 'p-1 text-sm gap-1',
        compact: 'p-[5px]',          // Reduced padding to compensate for larger icon
        mobile: 'p-2.5',
        card: 'p-3 gap-3',
    }[variant]);

    // Icon size based on variant
    const iconSizeClass = $derived({
        desktop: 'text-lg',
        compact: 'text-lg',
        mobile: 'text-2xl',
        card: 'text-2xl',
    }[variant]);

    // Layout differs for card (full-width, left-aligned, multi-line) vs the
    // inline action variants (centered, single line).
    const layoutClasses = $derived({
        desktop: 'justify-center whitespace-nowrap',
        compact: 'justify-center whitespace-nowrap',
        mobile: 'justify-center whitespace-nowrap',
        card: 'justify-start w-full text-left',
    }[variant]);
</script>

<button
    class="text-primary flex bg-surface1 border-overlay1 border-solid border-1 rounded-md items-center transition-colors {sizeClasses} {layoutClasses} {disabled ? 'cursor-not-allowed opacity-50' : 'cursor-pointer hover:bg-surface2 hover:border-mauve active:bg-surface3 focus:bg-surface2 focus:border-mauve focus:outline-none'} {className}"
    {disabled}
    title={effectiveTooltip}
    aria-label={effectiveTooltip}
    onclick={(event) => {
        event.stopPropagation();
        if (!disabled) {
            onClick();
        }
    }}
>
    {#if variant === 'desktop'}
        {#if position === 'left'}
            <div class="{icon} {iconSizeClass}"></div>
            <p>{text}</p>
        {:else}
            <p>{text}</p>
            <div class="{icon} {iconSizeClass}"></div>
        {/if}
    {:else if variant === 'card'}
        <div class="{icon} {iconSizeClass} flex-shrink-0 text-subtitle"></div>
        <div class="flex-1 min-w-0">
            <div class="font-medium text-primary">{text}</div>
            {#if subtitle}
                <div class="text-sm text-muted truncate">{subtitle}</div>
            {/if}
        </div>
        {#if trailing || trailingText}
            <div class="flex items-center gap-2 flex-shrink-0">
                {#if trailingText}<span class="text-xs text-subtitle">{trailingText}</span>{/if}
                {#if trailing}<div class="{trailing} {trailingClass} text-xl"></div>{/if}
            </div>
        {/if}
    {:else}
        <!-- Compact and mobile variants show icon only -->
        <div class="{icon} {iconSizeClass}"></div>
    {/if}
</button>
