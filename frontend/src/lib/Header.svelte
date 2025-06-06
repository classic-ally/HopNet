<script lang="ts">
    export let title: string;
    export let body: string;
</script>

<div class="flex flex-col gap-2">
    <h1 class="text-2xl">{title}</h1>
    <p>{body}</p>
</div>