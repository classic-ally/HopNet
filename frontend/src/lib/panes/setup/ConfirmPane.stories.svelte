<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import ConfirmPane from './ConfirmPane.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Setup/ConfirmPane',
    component: ConfirmPane,
    argTypes: {
      username: { control: 'text', description: 'Username to confirm' },
      computername: { control: 'text', description: 'Device name to confirm' },
      onBackButton: { action: 'onBackButton' },
      onSetupComplete: { action: 'onSetupComplete' }
    },
    parameters: {
      docs: {
        description: {
          component: 'The Save button triggers a real POST to /setup. In Storybook this will fail without a backend.'
        }
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <ConfirmPane
        onBackButton={() => console.log('Back clicked')}
        onSetupComplete={(pp) => console.log('Setup complete, passphrase:', pp)}
        {...args}
      />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ username: 'alice', computername: 'allison-macbook' }} />
