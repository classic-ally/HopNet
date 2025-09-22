<script lang="ts">
    export let icon: String;
    export let title: String;
    export let onClick: () => void;
    export let disabled: boolean = false;
</script>

<button
    class="border-1 border-overlay1 text-muted border-solid rounded-md p-1 bg-transparent {disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer hover:border-mauve hover:text-mauve hover:bg-surface0'}"
    title={title}
    onclick={disabled ? () => {} : onClick}
    disabled={disabled}
    aria-label={title}
>
    <div class={icon+ " text-2xl"}></div>
</button>