<script module>
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import ResiliencePane from './ResiliencePane.svelte';

  const { Story } = defineMeta({
    title: 'Panes/ResiliencePane',
    component: ResiliencePane,
    parameters: {
      layout: 'fullscreen',
      backgrounds: {
        default: 'dark',
        values: [
          { name: 'dark', value: '#1e1e2e' },
          { name: 'light', value: '#ffffff' }
        ]
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base">
    <ResiliencePane {...args} />
  </div>
{/snippet}

<Story
  name="Default"
  {template}
  args={{}}
/>

<Story
  name="Full Screen Demo"
  {template}
  args={{}}
  parameters={{
    docs: {
      description: {
        story: 'The ResiliencePane component displays fault tolerance charts, consensus health indicators, and network configuration tools for managing HopNet deployment resilience.'
      }
    }
  }}
/>