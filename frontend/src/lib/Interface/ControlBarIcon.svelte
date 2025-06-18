<script lang="ts">
    export let icon: String;
    export let title: String;
</script>

<div 
    class="border-1 border-indigo-500 text-indigo-500 border-solid rounded-md p-1 cursor-pointer"
    title={title}
    role="button"
>
    <div class={icon+ " text-2xl"}></div>
</div>