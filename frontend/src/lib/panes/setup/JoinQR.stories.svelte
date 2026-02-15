<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import JoinQR from './JoinQR.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Setup/JoinQR',
    component: JoinQR,
    argTypes: {
      name: { control: 'text', description: 'Device name for QR code' }
    },
    parameters: {
      docs: {
        description: {
          component: 'Fetches the public key from GET /setup on mount. In Storybook this will fail without a backend.'
        }
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6 flex items-center justify-center">
    <div class="w-full max-w-md">
      <JoinQR {...args} />
    </div>
  </div>
{/snippet}

<Story name="Default" {template} args={{ name: 'allison-macbook' }} />
