<script lang="ts">
    export let icon: String;
    export let title: String;
    export let onClick: () => void;
</script>

<button 
    class="border-1 border-indigo-500 text-indigo-500 border-solid rounded-md p-1 cursor-pointer bg-transparent"
    title={title}
    onclick={onClick}
    aria-label={title}
>
    <div class={icon+ " text-2xl"}></div>
</button>