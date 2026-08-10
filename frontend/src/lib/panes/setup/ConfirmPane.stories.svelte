<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import ConfirmPane from './ConfirmPane.svelte';
  import { mockSetupApi } from '../../api/setup.mock';
  import { TRANSPORT_FAILURE } from '../../api/setup';

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
          component:
            'Save runs genesis against a mock API, so the spinner and the minted passphrase ' +
            'are both observable without a backend.'
        }
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <ConfirmPane
        api={mockSetupApi()}
        onBackButton={() => console.log('Back clicked')}
        onSetupComplete={(pp) => console.log('Setup complete, passphrase:', pp)}
        {...args}
      />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ username: 'alice', computername: 'allison-macbook' }} />

{#snippet failing(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <ConfirmPane
        api={mockSetupApi({ failWith: args.failWith })}
        onBackButton={() => console.log('Back clicked')}
        onSetupComplete={(pp) => console.log('Setup complete, passphrase:', pp)}
        username={args.username}
        computername={args.computername}
      />
    </div>
  </div>
{/snippet}

<Story
  name="Genesis Rejected"
  template={failing}
  args={{ username: 'alice', computername: 'allison-macbook', failWith: 503 }}
/>

<Story
  name="No Backend"
  template={failing}
  args={{ username: 'alice', computername: 'allison-macbook', failWith: TRANSPORT_FAILURE }}
/>
