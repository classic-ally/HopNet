<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import AddDeviceModal from './AddDeviceModal.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Devices/AddDeviceModal',
        component: AddDeviceModal,
        argTypes: {
            isOpen: {
                control: 'boolean',
                description: 'Whether the modal is open'
            },
            onClose: {
                action: 'onClose',
                description: 'Called when modal is closed'
            },
            onDeviceAdded: {
                action: 'onDeviceAdded',
                description: 'Called when device is successfully registered'
            }
        }
    });
</script>

<script lang="ts">
    import type { RegisterDeviceResponse } from '../../types';

    function handleClose() {
        console.log('Modal closed');
    }

    function handleDeviceAdded(response: RegisterDeviceResponse, deviceName: string) {
        console.log('Device added:', deviceName, response);
    }
</script>

<!-- Default Story -->
<Story
    name="Default"
    args={{
        isOpen: true,
        onClose: handleClose,
        onDeviceAdded: handleDeviceAdded
    }}
/>
