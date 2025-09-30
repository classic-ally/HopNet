<script lang="ts">
  interface Tab {
    key: string;
    label: string;
    color: 'red' | 'yellow' | 'green' | 'blue' | 'mauve';
  }

  export let tabs: readonly Tab[] = [];
  export let activeTab: string = '';
  export let centered: boolean = false;
</script>

<div
  role="tablist"
  aria-label="Tab navigation"
  class="flex {centered ? 'justify-center' : ''} border border-solid border-overlay1 rounded-md overflow-visible relative z-20 bg-surface0"
>
  {#each tabs as tab, index}
    <div class="-m-px relative {
      index === 0 ? 'rounded-l-md' : ''
    } {
      index === tabs.length - 1 ? 'rounded-r-md' : ''
    }">
      <button
        class="px-4 text-sm transition-all border-b-2 border-solid w-full relative {
          activeTab === tab.key
            ? `${tab.color === 'mauve' ? 'text-primary' : `text-${tab.color}`} border-${tab.color} opacity-100 rounded-md bg-surface2 active:bg-surface3`
            : `${tab.color === 'mauve' ? 'text-primary' : `text-${tab.color}`} border-transparent opacity-60 hover:opacity-80 hover:bg-surface2 hover:rounded-md hover:border-${tab.color}/80 bg-transparent active:bg-surface1`
        }"
        style="padding-top: 3px; padding-bottom: 3px;"
        on:click={() => activeTab = tab.key}
        aria-label="Switch to {tab.label} tab"
        role="tab"
        aria-selected={activeTab === tab.key}
      >
        {tab.label}
      </button>
    </div>
  {/each}
</div>