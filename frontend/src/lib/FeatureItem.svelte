<script lang="ts">
  export let icon: string; // e.g. "i-carbon-3d-print-mesh"
  export let text: string;
</script>

<div class="grid grid-cols-[auto_1fr] items-center gap-3 border-indigo-500 border-solid border rounded-lg px-2 py-2">
  <div class={icon + " text-3xl"}></div>
  <p class="text-sm">{text}</p>
</div>
