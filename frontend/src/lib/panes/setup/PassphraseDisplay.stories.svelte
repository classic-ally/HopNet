<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import PassphraseDisplay from './PassphraseDisplay.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Setup/PassphraseDisplay',
    component: PassphraseDisplay,
    argTypes: {
      passphrase: { control: 'text', description: 'Generated passphrase' },
      onContinue: { action: 'onContinue' }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <PassphraseDisplay
        onContinue={() => console.log('Continue clicked')}
        {...args}
      />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ passphrase: 'abacus dolphin railway prewar reunion crystal sabotage gravitate' }} />
