<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import CreateNetwork from './CreateNetwork.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Setup/CreateNetwork',
    component: CreateNetwork,
    argTypes: {
      username: { control: 'text', description: 'Username field value' },
      onBackButton: { action: 'onBackButton' },
      onForwardButton: { action: 'onForwardButton' }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <CreateNetwork
        onBackButton={() => console.log('Back clicked')}
        onForwardButton={() => console.log('Forward clicked')}
        {...args}
      />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ username: '' }} />

<Story name="Pre-filled" {template} args={{ username: 'alice' }} />
