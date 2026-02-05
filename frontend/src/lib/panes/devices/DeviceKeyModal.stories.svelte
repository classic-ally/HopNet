<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import DeviceKeyModal from './DeviceKeyModal.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Devices/DeviceKeyModal',
        component: DeviceKeyModal,
        argTypes: {
            isOpen: {
                control: 'boolean',
                description: 'Whether the modal is open'
            },
            deviceName: {
                control: 'text',
                description: 'Name of the registered device'
            },
            apiKey: {
                control: 'text',
                description: 'The API key to display'
            },
            onClose: {
                action: 'onClose',
                description: 'Called when modal is closed'
            }
        }
    });
</script>

<script lang="ts">
    function handleClose() {
        console.log('Modal closed');
    }
</script>

<!-- Default Story -->
<Story
    name="Default"
    args={{
        isOpen: true,
        deviceName: 'My Android Phone',
        apiKey: '0194ab7c-8e3f-7d2a-9b1c-4f5e6d7c8b9a.a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6',
        onClose: handleClose
    }}
/>

<!-- Long Key Story -->
<Story
    name="Long Key"
    args={{
        isOpen: true,
        deviceName: 'Work Tablet with Very Long Device Name',
        apiKey: '0194ab7c-8e3f-7d2a-9b1c-4f5e6d7c8b9a.a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6a7b8c9d0e1f2',
        onClose: handleClose
    }}
/>

<!-- Short Device Name Story -->
<Story
    name="Short Device Name"
    args={{
        isOpen: true,
        deviceName: 'iPad',
        apiKey: '0194ab7c-0001-0000-0000-000000000001.secretkey123',
        onClose: handleClose
    }}
/>
