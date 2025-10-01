<script lang="ts">
    interface ButtonProps {
        icon: string;
        text: string;
        onClick: () => void;
        tooltip?: string;
        variant?: 'desktop' | 'compact' | 'mobile';
        position?: 'left' | 'right';
        className?: string;
        disabled?: boolean;
    }

    let {
        icon,
        text,
        onClick,
        tooltip,
        variant = 'desktop',
        position = 'left',
        className = '',
        disabled = false
    }: ButtonProps = $props();

    // Effective tooltip is either the override or the button text
    const effectiveTooltip = $derived(tooltip || text);

    // Size classes based on variant
    const sizeClasses = $derived({
        desktop: 'p-1 text-sm gap-1',
        compact: 'p-[5px]',          // Reduced padding to compensate for larger icon
        mobile: 'p-2.5'
    }[variant]);

    // Icon size based on variant
    const iconSizeClass = $derived({
        desktop: 'text-lg',           // Default size
        compact: 'text-lg',   // Slightly larger
        mobile: 'text-2xl'    // Touch-friendly size
    }[variant]);
</script>

<button
    class="text-primary justify-center flex bg-surface1 border-overlay1 border-solid border-1 rounded-md items-center transition-colors whitespace-nowrap {sizeClasses} {disabled ? 'cursor-not-allowed opacity-50' : 'cursor-pointer hover:bg-surface2 hover:border-mauve active:bg-surface3 focus:bg-surface2 focus:border-mauve focus:outline-none'} {className}"
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
    {:else}
        <!-- Compact and mobile variants show icon only -->
        <div class="{icon} {iconSizeClass}"></div>
    {/if}
</button>