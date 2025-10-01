<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import Modal from './Modal.svelte';
    import Button from '../Button.svelte';

    const { Story } = defineMeta({
        title: 'Primitives/Modal',
        component: Modal,
        argTypes: {
            size: {
                control: 'radio',
                options: ['sm', 'md', 'lg', 'xl'],
                description: 'Modal size'
            },
            mode: {
                control: 'radio',
                options: ['desktop', 'mobile'],
                description: 'Device mode for close button sizing'
            },
            onClose: {
                action: 'onClose',
                description: 'Function called when modal is closed'
            },
            loading: {
                control: 'boolean',
                description: 'Loading state'
            },
            error: {
                control: 'text',
                description: 'Error message'
            },
            success: {
                control: 'text',
                description: 'Success message'
            }
        }
    });
</script>

<script lang="ts">
    function handleModalClose() {
        console.log('Story: handleModalClose called');
    }

    // Debug the function at story level
    console.log('Story: handleModalClose type:', typeof handleModalClose);
    console.log('Story: handleModalClose value:', handleModalClose);
</script>

<!-- Simple Modal Story -->
<Story name="Simple Modal" args={{ title: "Simple Information", onClose: handleModalClose }}>
    {#snippet template(args)}
        <Modal {...args}>
            {#snippet content()}
                <div class="space-y-4">
                    <p class="text-primary">This is a simple modal with basic content.</p>
                    <p class="text-muted text-sm">It demonstrates the most basic usage of the Modal component.</p>
                </div>
            {/snippet}

            {#snippet footer()}
                <div class="flex justify-end gap-2">
                    <Button
                        icon="i-carbon-close"
                        text="Cancel"
                        onClick={() => console.log('Cancel clicked')}
                    />
                    <Button
                        icon="i-carbon-checkmark"
                        text="Got it"
                        onClick={() => console.log('Got it clicked')}
                    />
                </div>
            {/snippet}
        </Modal>
    {/snippet}
</Story>