<script lang="ts">
    export let icon: string;
    export let text: string;
    export let onClick: () => void;
    export let position: 'left' | 'right' = 'left'; // Optional, default is 'left'
    export let className: string = ''; // Optional additional classes
    export let disabled: boolean = false; // Optional disabled state
</script>

<button
    class="text-primary justify-center flex bg-surface1 p-1 border-overlay1 border-solid border-1 rounded-md text-sm items-center gap-1 transition-colors {disabled ? 'cursor-not-allowed opacity-50' : 'cursor-pointer hover:bg-surface2 hover:border-mauve active:bg-surface3'} {className}"
    {disabled}
    on:click={(event) => {
        event.stopPropagation();
        if (!disabled) {
            onClick();
        }
    }}
>
    {#if position === 'left'}
        <div class={icon}></div>
        <p>{text}</p>
    {:else}
        <p>{text}</p>
        <div class={icon}></div>
    {/if}
</button>