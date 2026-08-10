<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import IncomingSharesPane from './IncomingSharesPane.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Shares/IncomingSharesPane',
        component: IncomingSharesPane,
        argTypes: {
            onToggleSidebar: {
                action: 'onToggleSidebar',
                description: 'Called when sidebar toggle is requested'
            }
        },
        parameters: {
            docs: {
                description: {
                    component:
                        'Files other users have shared with you, and the accept flow. ' +
                        'Makes real API calls, so with no backend it settles into its error or ' +
                        'empty state — which is enough to review the pane header and chrome.'
                }
            }
        }
    });
</script>

<!--
  The app mounts every pane inside Interface, which supplies the page padding and
  the surrounding chrome. Reproduced here so the header does not float against
  the viewport edge and misrepresent its spacing.
-->
{#snippet template(args)}
    <div class="p-5 flex flex-col gap-3 w-full">
        <IncomingSharesPane {...args} />
    </div>
{/snippet}

<Story name="Default" {template} args={{}} />
