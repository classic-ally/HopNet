<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import JoinCodeEntry from './JoinCodeEntry.svelte';
  import { mockSetupApi } from '../../api/setup.mock';
  import { TRANSPORT_FAILURE } from '../../api/setup';

  const { Story } = defineMeta({
    title: 'Panes/Setup/JoinCodeEntry',
    component: JoinCodeEntry,
    argTypes: {
      onBackButton: { action: 'onBackButton' },
      onCodeAccepted: { action: 'onCodeAccepted' }
    },
    parameters: {
      docs: {
        description: {
          component:
            'The mesh-code entry step (RFC-025 S5) that precedes the pairing QR. ' +
            'Entering the code is what makes this device reachable over the mesh ' +
            'transport, so the flow cannot advance without it. Input formats to ' +
            'XXXX-XXXX as you type; the server parser is equally tolerant.'
        }
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <JoinCodeEntry
        api={mockSetupApi()}
        onBackButton={() => console.log('back')}
        onCodeAccepted={() => console.log('accepted')}
        {...args}
      />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{}} />

{#snippet failing(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <JoinCodeEntry
        api={mockSetupApi({ failWith: args.failWith })}
        onBackButton={() => console.log('back')}
        onCodeAccepted={() => console.log('accepted')}
      />
    </div>
  </div>
{/snippet}

<!-- 409: a different code was already adopted — restart is the remedy. -->
<Story name="Conflicting Code" template={failing} args={{ failWith: 409 }} />

<Story name="No Backend" template={failing} args={{ failWith: TRANSPORT_FAILURE }} />
