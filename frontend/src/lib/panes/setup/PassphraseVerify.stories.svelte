<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import PassphraseVerify from './PassphraseVerify.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Setup/PassphraseVerify',
    component: PassphraseVerify,
    argTypes: {
      passphrase: { control: 'text', description: 'Passphrase to verify against' },
      onVerified: { action: 'onVerified' }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <PassphraseVerify
        onVerified={() => console.log('Verified')}
        {...args}
      />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ passphrase: 'abacus dolphin railway prewar reunion crystal sabotage gravitate' }} />
