<script lang="ts">
    interface TextInputProps {
        value?: string;
        placeholder?: string;
        disabled?: boolean;
        type?: 'text' | 'password' | 'email' | 'number';
        mode?: 'desktop' | 'mobile';
        className?: string;
        oninput?: (event: Event) => void;
        onkeydown?: (event: KeyboardEvent) => void;
    }

    let {
        value = '',
        placeholder = '',
        disabled = false,
        type = 'text',
        mode = 'desktop',
        className = '',
        oninput,
        onkeydown
    }: TextInputProps = $props();

    // Use internal state that syncs with prop
    let internalValue = $state(value);

    // Sync internal value with prop changes
    $effect(() => {
        internalValue = value;
    });

    // Size classes based on mode to match Button heights and font sizes
    const sizeClasses = $derived({
        desktop: 'px-3 py-1.5 text-base',  // Increased padding to compensate for text-base line height
        mobile: 'px-3 py-2.5 text-base'    // Matches Button mobile height and text size
    }[mode]);
</script>

<input
    class="w-full box-border bg-surface1 border-overlay1 border-solid border-1 rounded-md text-primary transition-colors hover:border-mauve focus:bg-surface2 focus:border-mauve focus:outline-none placeholder:text-muted font-inherit {sizeClasses} {disabled ? 'cursor-not-allowed opacity-50' : ''} {className}"
    {type}
    {placeholder}
    {disabled}
    bind:value={internalValue}
    oninput={(e) => {
        if (oninput) oninput(e);
    }}
    onkeydown={(e) => {
        if (onkeydown) onkeydown(e);
    }}
/>