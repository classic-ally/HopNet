<script lang="ts">
    export let icon: String;
    export let title: String;
    export let selected: Boolean;
    export let onClick: () => void;
</script>

<button 
    class={`text-white flex p-1 gap-2 items-center cursor-pointer border-solid border-1 rounded-md ${selected ? 'border-indigo-500 bg-indigo-950' : 'bg-transparent border-transparent'}`}
    onclick={onClick}
>
    <div class={icon + " text-lg"}></div>
    <div class="text-sm">{title}</div>
</button>