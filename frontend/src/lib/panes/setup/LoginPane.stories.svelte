<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import LoginPane from './LoginPane.svelte';
  import { mockSetupApi } from '../../api/setup.mock';
  import { TRANSPORT_FAILURE } from '../../api/setup';

  const { Story } = defineMeta({
    title: 'Panes/Setup/LoginPane',
    component: LoginPane,
    argTypes: {
      username: {
        control: 'text',
        description: 'Pre-filled username'
      },
      passphrase: {
        control: 'text',
        description: 'Pre-filled passphrase'
      }
    },
    parameters: {
      docs: {
        description: {
          component:
            'Login runs against a mock API. The failure stories cover each status the pane ' +
            'maps to its own message.'
        }
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <LoginPane api={mockSetupApi()} {...args} />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ username: '', passphrase: '' }} />

<Story name="Pre-filled" {template} args={{ username: 'alice', passphrase: 'abacus dolphin railway prewar reunion crystal sabotage gravitate' }} />

<!--
  Each of these needs its own api instance, so the failure is bound per story
  rather than shared through args.
-->
{#snippet failing(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <LoginPane
        api={mockSetupApi({ failWith: args.failWith })}
        username={args.username}
        passphrase={args.passphrase}
      />
    </div>
  </div>
{/snippet}

<Story
  name="Bad Credentials"
  template={failing}
  args={{ username: 'alice', passphrase: 'wrong words entirely', failWith: 401 }}
/>

<Story
  name="Node Not Initialized"
  template={failing}
  args={{ username: 'alice', passphrase: 'abacus dolphin railway prewar', failWith: 503 }}
/>

<Story
  name="No Backend"
  template={failing}
  args={{ username: 'alice', passphrase: 'abacus dolphin railway prewar', failWith: TRANSPORT_FAILURE }}
/>
