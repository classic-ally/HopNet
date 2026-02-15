<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import PassphraseInput from './PassphraseInput.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Setup/PassphraseInput',
    component: PassphraseInput,
    argTypes: {
      value: {
        control: 'text',
        description: 'Space-joined passphrase value'
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md bg-surface0 rounded-lg p-4">
      <PassphraseInput {...args} />
    </div>
  </div>
{/snippet}

<Story name="Empty" {template} args={{ value: '' }} />

<Story name="Partial" {template} args={{ value: 'abacus dolphin railway' }} />

<Story name="Complete" {template} args={{ value: 'abacus dolphin railway prewar reunion crystal sabotage gravitate' }} />

<Story name="With Errors" {template} args={{ value: 'abacus dolphin xyzzy prewar badword crystal sabotage gravitate' }} />
