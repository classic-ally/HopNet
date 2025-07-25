<script lang="ts">
    export let variant: 'primary' | 'danger' = 'primary';
    export let disabled: boolean = false;
    export let type: 'button' | 'submit' = 'button';
    export let onclick: (() => void) | undefined = undefined;
    
    $: buttonClasses = `px-4 py-2 rounded-md border-none transition-colors whitespace-nowrap font-medium ${
        variant === 'primary' 
            ? 'bg-mauve hover:bg-mauve/80 disabled:bg-surface2 text-crust' 
            : 'bg-red hover:bg-red/80 disabled:bg-surface2 text-crust'
    } ${disabled ? 'cursor-not-allowed opacity-75' : 'cursor-pointer'}`;
</script>

<button 
    class={buttonClasses}
    {type}
    {disabled}
    on:click={onclick}
>
    <slot />
</button>