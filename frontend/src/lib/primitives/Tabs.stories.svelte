<script module>
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import Tabs from './Tabs.svelte';
  import Button from '../Button.svelte';

  const { Story } = defineMeta({
    title: 'Primitives/Tabs',
    component: Tabs,
    argTypes: {
      activeTab: { control: 'text' },
      centered: { control: 'boolean' },
      tabs: { control: 'object' }
    }
  });

  // Sample tab configurations
  const securityTabs = [
    { key: 'setup', label: 'Setup Mode (1-2 validators)', color: 'red' },
    { key: 'crash', label: 'Crash Protection (3-6 validators)', color: 'yellow' },
    { key: 'anomaly', label: 'Crash + Anomaly Protection (7+ validators)', color: 'green' }
  ];

  const simpleTabs = [
    { key: 'overview', label: 'Overview', color: 'mauve' },
    { key: 'details', label: 'Details', color: 'mauve' },
    { key: 'settings', label: 'Settings', color: 'mauve' }
  ];

  const statusTabs = [
    { key: 'error', label: 'Errors', color: 'red' },
    { key: 'warning', label: 'Warnings', color: 'yellow' },
    { key: 'success', label: 'Success', color: 'green' },
    { key: 'info', label: 'Info', color: 'blue' }
  ];
</script>

{#snippet template(args)}
  <div class="p-4 bg-base min-h-screen">
    <div class="max-w-4xl mx-auto">
      <h3 class="text-lg font-semibold text-primary mb-4">Tab Component</h3>
      <Tabs {...args} />
      <div class="mt-4 p-4 bg-surface0 rounded-lg">
        <p class="text-text text-sm">Active tab: <span class="font-mono text-primary">{args.activeTab}</span></p>
      </div>

      <!-- Visual comparison with Button -->
      <div class="mt-6 p-4 bg-surface0 rounded-lg">
        <h4 class="text-md font-medium text-primary mb-3">Comparison with Button Component</h4>
        <div class="flex items-center gap-2">
          <Tabs {...args} />
          <Button
            icon="i-carbon-add"
            text="Sample Button"
            onClick={() => console.log('Button clicked')}
          />
          <span class="text-subtitle text-sm">← Same vertical height and corner radius</span>
        </div>
      </div>
    </div>
  </div>
{/snippet}

<Story
  name="Security Tabs"
  {template}
  args={{
    tabs: securityTabs,
    activeTab: 'crash',
    centered: true
  }}
/>

<Story
  name="Simple Tabs"
  {template}
  args={{
    tabs: simpleTabs,
    activeTab: 'overview',
    centered: false
  }}
/>

<Story
  name="Status Tabs"
  {template}
  args={{
    tabs: statusTabs,
    activeTab: 'warning',
    centered: true
  }}
/>

<Story
  name="Single Tab"
  {template}
  args={{
    tabs: [{ key: 'only', label: 'Only Tab', color: 'blue' }],
    activeTab: 'only',
    centered: false
  }}
/>

<Story
  name="No Tabs"
  {template}
  args={{
    tabs: [],
    activeTab: '',
    centered: false
  }}
/>