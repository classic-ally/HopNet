<script module>
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import SecurityPropertiesPage from './SecurityPropertiesPage.svelte';

  const { Story } = defineMeta({
    title: 'Pages/SecurityPropertiesPage',
    component: SecurityPropertiesPage,
    argTypes: {
      activeValidators: {
        control: { type: 'range', min: 1, max: 40, step: 1 },
        description: 'Number of active validators (affects which tab is auto-selected)'
      }
    }
  });
</script>

{#snippet template(args)}
  <SecurityPropertiesPage {...args} />
{/snippet}

<Story
  name="Setup Mode"
  {template}
  args={{
    activeValidators: 1
  }}
/>

<Story
  name="Crash Protection Mode"
  {template}
  args={{
    activeValidators: 5
  }}
/>

<Story
  name="Crash + Anomaly Protection Mode"
  {template}
  args={{
    activeValidators: 9
  }}
/>