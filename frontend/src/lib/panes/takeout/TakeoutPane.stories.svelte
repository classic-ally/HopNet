<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import TakeoutPane from './TakeoutPane.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Takeout/TakeoutPane',
        component: TakeoutPane,
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
                        'Data export requests and their download state. ' +
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
        <TakeoutPane {...args} />
    </div>
{/snippet}

<Story name="Default" {template} args={{}} />
