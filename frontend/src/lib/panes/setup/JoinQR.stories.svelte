<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import JoinQR from './JoinQR.svelte';
  import { mockSetupApi } from '../../api/setup.mock';
  import { TRANSPORT_FAILURE } from '../../api/setup';

  const { Story } = defineMeta({
    title: 'Panes/Setup/JoinQR',
    component: JoinQR,
    argTypes: {
      onBackButton: { action: 'onBackButton' }
    },
    parameters: {
      docs: {
        description: {
          component:
            'Fetches the public key on mount from a mock API, so the loading, loaded and error ' +
            'states are all reachable without a backend.'
        }
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <JoinQR api={mockSetupApi()} onBackButton={() => console.log("back")} {...args} />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{}} />

<!-- Long latency parks the pane in its loading state so it can be inspected. -->
{#snippet loading(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <JoinQR api={mockSetupApi({ latencyMs: 600_000 })} />
    </div>
  </div>
{/snippet}

<Story name="Loading" template={loading} args={{}} />

{#snippet failing(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <JoinQR api={mockSetupApi({ failWith: args.failWith })} />
    </div>
  </div>
{/snippet}

<Story
  name="Fetch Rejected"
  template={failing}
  args={{ failWith: 500 }}
/>

<Story
  name="No Backend"
  template={failing}
  args={{ failWith: TRANSPORT_FAILURE }}
/>
