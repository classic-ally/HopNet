<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import LoginPane from './LoginPane.svelte';

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
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <LoginPane {...args} />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ username: '', passphrase: '' }} />

<Story name="Pre-filled" {template} args={{ username: 'alice', passphrase: 'abacus dolphin railway prewar reunion crystal sabotage gravitate' }} />
