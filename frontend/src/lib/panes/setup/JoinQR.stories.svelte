<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import JoinQR from './JoinQR.svelte';
  import { mockSetupApi } from '../../api/setup.mock';
  import { TRANSPORT_FAILURE } from '../../api/setup';

  const { Story } = defineMeta({
    title: 'Panes/Setup/JoinQR',
    component: JoinQR,
    argTypes: {
      name: { control: 'text', description: 'Device name for QR code' }
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
      <JoinQR api={mockSetupApi()} {...args} />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ name: 'allison-macbook' }} />

<!-- Long latency parks the pane in its loading state so it can be inspected. -->
{#snippet loading(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <JoinQR api={mockSetupApi({ latencyMs: 600_000 })} name={args.name} />
    </div>
  </div>
{/snippet}

<Story name="Loading" template={loading} args={{ name: 'allison-macbook' }} />

{#snippet failing(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <JoinQR api={mockSetupApi({ failWith: args.failWith })} name={args.name} />
    </div>
  </div>
{/snippet}

<Story
  name="Fetch Rejected"
  template={failing}
  args={{ name: 'allison-macbook', failWith: 500 }}
/>

<Story
  name="No Backend"
  template={failing}
  args={{ name: 'allison-macbook', failWith: TRANSPORT_FAILURE }}
/>
