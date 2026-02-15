<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import ConfigureDevice from './ConfigureDevice.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Setup/ConfigureDevice',
    component: ConfigureDevice,
    argTypes: {
      computername: { control: 'text', description: 'Device name field value' },
      onBackButton: { action: 'onBackButton' },
      onForwardButton: { action: 'onForwardButton' }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <ConfigureDevice
        onBackButton={() => console.log('Back clicked')}
        onForwardButton={() => console.log('Forward clicked')}
        {...args}
      />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ computername: '' }} />

<Story name="Pre-filled" {template} args={{ computername: 'allison-macbook' }} />
