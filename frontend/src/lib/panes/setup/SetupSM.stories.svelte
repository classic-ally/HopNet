<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import SetupSM from './SetupSM.svelte';
  import { mockSetupApi, MOCK_PASSPHRASE } from '../../api/setup.mock';
  import { TRANSPORT_FAILURE } from '../../api/setup';

  const { Story } = defineMeta({
    title: 'Panes/Setup/SetupSM',
    component: SetupSM,
    argTypes: {
      step: {
        control: 'select',
        options: [
          'initial',
          'create-network',
          'confirm',
          'configure-device',
          'join-qr',
          'passphrase-display',
          'passphrase-verify'
        ],
        description: 'Which step to enter at'
      },
      username: { control: 'text' },
      computername: { control: 'text' },
      passphrase: { control: 'text' }
    },
    parameters: {
      docs: {
        description: {
          component:
            'The full setup flow, driven end to end against a mock API — no backend required. ' +
            'Both branches are walkable: create runs through the passphrase ceremony, join ends ' +
            'at the pairing QR. The mock adds latency so the spinners and disabled states are visible.'
        }
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <SetupSM
        api={mockSetupApi()}
        onComplete={() => console.log('Setup complete — the real app reloads here')}
        {...args}
      />
    </div>
  </div>
{/snippet}

<!--
  Walk it: Create a Network → fill both fields → Save (mock mints the
  passphrase) → read three words off the display → type them into verify.
-->
<Story name="Full Flow" {template} args={{ step: 'initial' }} />

<Story name="Create — Details" {template} args={{ step: 'create-network' }} />

<Story
  name="Create — Confirm"
  {template}
  args={{ step: 'confirm', username: 'allison', computername: 'laptop' }}
/>

<Story
  name="Create — Passphrase Display"
  {template}
  args={{ step: 'passphrase-display', passphrase: MOCK_PASSPHRASE }}
/>

<Story
  name="Create — Passphrase Verify"
  {template}
  args={{ step: 'passphrase-verify', username: 'allison', passphrase: MOCK_PASSPHRASE }}
/>

<Story name="Join — Device Name" {template} args={{ step: 'configure-device' }} />

<Story name="Join — Pairing QR" {template} args={{ step: 'join-qr', computername: 'laptop' }} />

<!-- Failure paths: the two the flow actually distinguishes. -->
{#snippet failing(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <SetupSM
        api={mockSetupApi({ failWith: args.failWith })}
        onComplete={() => console.log('Setup complete — the real app reloads here')}
        step={args.step}
        username={args.username}
        computername={args.computername}
      />
    </div>
  </div>
{/snippet}

<Story
  name="Create — Genesis Rejected"
  template={failing}
  args={{ step: 'confirm', username: 'allison', computername: 'laptop', failWith: 503 }}
/>

<Story
  name="Join — No Backend"
  template={failing}
  args={{ step: 'join-qr', computername: 'laptop', failWith: TRANSPORT_FAILURE }}
/>
