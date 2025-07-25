<script lang="ts">
    export let icon: String;
    export let title: String;
    export let selected: Boolean;
    export let onClick: () => void;
</script>

<button 
    class={`flex p-1 gap-2 items-center cursor-pointer border-solid border-1 rounded-md ${selected ? 'border-mauve bg-surface0 text-primary' : 'bg-transparent border-transparent hover:bg-surface0 text-muted hover:text-primary'}`}
    onclick={onClick}
>
    <div class={icon + ` text-lg ${selected ? 'text-mauve' : 'text-muted hover:text-mauve'}`}></div>
    <div class="text-sm">{title}</div>
</button>