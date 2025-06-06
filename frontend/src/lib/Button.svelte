<script lang="ts">
    export let icon: string;
    export let text: string;
    export let onClick: () => void;
    export let position: 'left' | 'right' = 'left'; // Optional, default is 'left'
</script>

<button 
    class="rounded-4 p-2 bg-blue-950 border border-solid border-blue-800 font-sans text-white text-base flex justify-center items-center gap-2 cursor-pointer"
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