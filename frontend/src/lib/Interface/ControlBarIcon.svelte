<script lang="ts">
    export let icon: String;
    export let title: String;
    export let onClick: () => void;
</script>

<button 
    class="border-1 border-overlay1 text-muted border-solid rounded-md p-1 cursor-pointer bg-transparent hover:border-mauve hover:text-mauve hover:bg-surface0"
    title={title}
    onclick={onClick}
    aria-label={title}
>
    <div class={icon+ " text-2xl"}></div>
</button>