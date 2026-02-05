<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import DevicesPane from './DevicesPane.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Devices/DevicesPane',
        component: DevicesPane,
        argTypes: {
            onToggleSidebar: {
                action: 'onToggleSidebar',
                description: 'Called when sidebar toggle is requested'
            }
        },
        parameters: {
            // Note: This component makes real API calls.
            // In a real test environment, you would mock the API.
            docs: {
                description: {
                    component: 'Main device management pane with table, toolbar, and modals.'
                }
            }
        }
    });
</script>

<script lang="ts">
    function handleToggleSidebar() {
        console.log('Toggle sidebar');
    }
</script>

<!-- Default Story -->
<Story
    name="Default"
    args={{
        onToggleSidebar: handleToggleSidebar
    }}
/>
