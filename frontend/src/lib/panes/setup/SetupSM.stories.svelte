<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import SetupSM from './SetupSM.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Setup/SetupSM',
    component: SetupSM,
    parameters: {
      docs: {
        description: {
          component: 'Full setup state machine. Navigation between steps works; the final Save and Join steps make real API calls that require a backend.'
        }
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <SetupSM {...args} />
    </div>
  </div>
{/snippet}

<Story name="Full Flow" {template} args={{}} />
