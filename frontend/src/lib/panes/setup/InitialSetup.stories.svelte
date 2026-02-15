<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import InitialSetup from './InitialSetup.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Setup/InitialSetup',
    component: InitialSetup,
    argTypes: {
      onCreateNetwork: { action: 'onCreateNetwork' },
      onJoinNetwork: { action: 'onJoinNetwork' }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <InitialSetup
        onCreateNetwork={() => console.log('Create Network clicked')}
        onJoinNetwork={() => console.log('Join Network clicked')}
        {...args}
      />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{}} />
