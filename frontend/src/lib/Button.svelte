<script lang="ts">
    export let icon: string;
    export let text: string;
    export let onClick: () => void;
    export let position: 'left' | 'right' = 'left'; // Optional, default is 'left'
</script>

<button 
    class="rounded-4 p-2 bg-surface0 border border-solid border-overlay1 font-sans text-primary text-sm flex justify-center items-center gap-2 cursor-pointer hover:bg-surface1 hover:border-mauve transition-colors"
    onclick={onClick}
>
    {#if position === 'left'}
        <div class={icon}></div>
        <p>{text}</p>
    {:else}
        <p>{text}</p>
        <div class={icon}></div>
    {/if}
</button>