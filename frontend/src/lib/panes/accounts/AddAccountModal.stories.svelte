<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import AddAccountModal from './AddAccountModal.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Accounts/AddAccountModal',
        component: AddAccountModal,
        argTypes: {
            isOpen: { control: 'boolean', description: 'Whether the modal is open' },
            onClose: { action: 'onClose', description: 'Called when modal closes' },
            onAccountCreated: { action: 'onAccountCreated', description: 'Called when account is created' },
        }
    });
</script>

<script lang="ts">
    function handleClose() {
        console.log('Story: Modal closed');
    }

    function handleAccountCreated() {
        console.log('Story: Account created');
    }
</script>

<Story
    name="Username Input"
    args={{
        isOpen: true,
        onClose: handleClose,
        onAccountCreated: handleAccountCreated,
    }}
>
    {#snippet template(args)}
        <AddAccountModal {...args} />
    {/snippet}
</Story>
