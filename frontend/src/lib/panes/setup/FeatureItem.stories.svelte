<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import FeatureItem from './FeatureItem.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Setup/FeatureItem',
    component: FeatureItem,
    argTypes: {
      icon: {
        control: 'text',
        description: 'UnoCSS icon class'
      },
      text: {
        control: 'text',
        description: 'Feature description text'
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <FeatureItem {...args} />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ icon: 'i-carbon-3d-print-mesh', text: 'Create a network spanning all your devices, friends, or even an entire company.' }} />

<Story name="Security Feature" {template} args={{ icon: 'i-carbon-group-security', text: 'All data is encrypted with your user\'s key, so other members of the network can\'t read your files.' }} />
