<script lang="ts">
  import { createEventDispatcher } from 'svelte';

  interface Tab {
    key: string;
    label: string;
    icon?: string; // Optional icon for mobile mode
    color: 'red' | 'yellow' | 'green' | 'blue' | 'mauve';
  }

  export let tabs: readonly Tab[] = [];
  export let activeTab: string = '';
  export let centered: boolean = false;
  export let variant: 'desktop' | 'compact' | 'mobile' = 'desktop'; // Display variant

  const dispatch = createEventDispatcher();

  // Size classes based on variant - matching Button component heights
  $: sizeClasses = {
    desktop: 'px-4 py-[3px] text-sm gap-1',
    compact: 'p-[4px]',          // Reduced padding to compensate for larger icon
    mobile: 'p-2.5'
  }[variant];

  // Icon size based on variant - matching Button component
  $: iconSizeClass = {
    desktop: 'text-lg',
    compact: 'text-lg',           // Slightly larger
    mobile: 'text-2xl'            // Touch-friendly size
  }[variant];
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
        class="border-b-2 border-solid w-full relative flex items-center justify-center {sizeClasses} {
          activeTab === tab.key
            ? `${tab.color === 'mauve' ? 'text-primary' : `text-${tab.color}`} border-${tab.color} opacity-100 rounded-md bg-surface2 active:bg-surface3`
            : `${tab.color === 'mauve' ? 'text-primary' : `text-${tab.color}`} border-transparent opacity-60 hover:opacity-80 hover:bg-surface2 hover:rounded-md hover:border-${tab.color}/80 bg-transparent active:bg-surface1`
        }"
        on:click={() => {
          activeTab = tab.key;
          dispatch('tabClick', { key: tab.key });
        }}
        aria-label="Switch to {tab.label} tab"
        title={tab.label}
        role="tab"
        aria-selected={activeTab === tab.key}
      >
        {#if variant === 'desktop'}
          {#if tab.icon}
            <div class="{tab.icon} {iconSizeClass}"></div>
          {/if}
          <span>{tab.label}</span>
        {:else if variant === 'compact'}
          <!-- Compact variant: icon only, like Button component -->
          {#if tab.icon}
            <div class="{tab.icon} {iconSizeClass}"></div>
          {:else}
            <!-- Fallback to first letter if no icon in compact mode -->
            <span class="text-lg font-medium">{tab.label.charAt(0).toUpperCase()}</span>
          {/if}
        {:else}
          <!-- Mobile variant: icon only -->
          {#if tab.icon}
            <div class="{tab.icon} {iconSizeClass}"></div>
          {:else}
            <!-- Fallback to first letter if no icon in mobile mode -->
            <span class="text-xl font-medium">{tab.label.charAt(0).toUpperCase()}</span>
          {/if}
        {/if}
      </button>
    </div>
  {/each}
</div>