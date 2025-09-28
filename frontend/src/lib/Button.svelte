<script lang="ts">
    export let icon: string;
    export let text: string;
    export let onClick: () => void;
    export let tooltip: string | undefined = undefined; // Optional tooltip override
    export let variant: 'desktop' | 'compact' | 'mobile' = 'desktop'; // Display variant
    export let position: 'left' | 'right' = 'left'; // Icon position (desktop only)
    export let className: string = ''; // Optional additional classes
    export let disabled: boolean = false; // Optional disabled state

    // Effective tooltip is either the override or the button text
    $: effectiveTooltip = tooltip || text;

    // Size classes based on variant
    $: sizeClasses = {
        desktop: 'p-1 text-sm gap-1',
        compact: 'p-[5px]',          // Reduced padding to compensate for larger icon
        mobile: 'p-2.5'
    }[variant];

    // Icon size based on variant
    $: iconSizeClass = {
        desktop: 'text-lg',           // Default size
        compact: 'text-lg',   // Slightly larger
        mobile: 'text-2xl'    // Touch-friendly size
    }[variant];
</script>

<button
    class="text-primary justify-center flex bg-surface1 border-overlay1 border-solid border-1 rounded-md items-center transition-colors whitespace-nowrap {sizeClasses} {disabled ? 'cursor-not-allowed opacity-50' : 'cursor-pointer hover:bg-surface2 hover:border-mauve active:bg-surface3'} {className}"
    {disabled}
    title={effectiveTooltip}
    aria-label={effectiveTooltip}
    on:click={(event) => {
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