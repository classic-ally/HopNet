<script lang="ts">
    import Modal from '../../primitives/Modal.svelte';
    import TextInput from '../../primitives/TextInput.svelte';
    import Button from '../../Button.svelte';
    import PassphraseDisplayContent from '../../primitives/PassphraseDisplayContent.svelte';
    import PassphraseVerifyContent from '../../primitives/PassphraseVerifyContent.svelte';
    import { createAccount } from '../../api/accounts';

    interface AddAccountModalProps {
        isOpen: boolean;
        onClose: () => void;
        onAccountCreated: () => void;
    }

    let {
        isOpen,
        onClose,
        onAccountCreated
    }: AddAccountModalProps = $props();

    let step: 1 | 2 | 3 = $state(1);
    let username = $state('');
    let passphrase = $state('');
    let loading = $state(false);
    let error = $state('');
    let verifyRef: PassphraseVerifyContent = $state() as PassphraseVerifyContent;

    function resetState() {
        step = 1;
        username = '';
        passphrase = '';
        loading = false;
        error = '';
    }

    function handleClose() {
        resetState();
        onClose();
    }

    async function handleCreate() {
        if (!username.trim()) {
            error = 'Username is required';
            return;
        }

        loading = true;
        error = '';

        try {
            const result = await createAccount(username.trim());
            passphrase = result.passphrase;
            step = 2;
        } catch (err) {
            error = err instanceof Error ? err.message : 'Failed to create account';
        } finally {
            loading = false;
        }
    }

    function handleWrittenDown() {
        step = 3;
    }

    function handleVerify() {
        if (verifyRef.verify()) {
            onAccountCreated();
            handleClose();
        }
    }

    function handleKeydown(event: KeyboardEvent) {
        if (event.key === 'Enter' && !loading && step === 1) {
            handleCreate();
        }
    }

    let title = $derived(
        step === 1 ? 'Create Account' :
        step === 2 ? 'Save Your Passphrase' :
        'Verify Passphrase'
    );
</script>

{#if isOpen}
    <Modal
        {title}
        size="sm"
        onClose={handleClose}
        {loading}
        error={step === 1 ? error : ''}
    >
        {#snippet content()}
            {#if step === 1}
                <div class="space-y-4">
                    <div>
                        <label for="username" class="block text-muted text-sm mb-2">
                            Username
                        </label>
                        <TextInput
                            id="username"
                            value={username}
                            placeholder="e.g., alice"
                            disabled={loading}
                            oninput={(e) => username = (e.target as HTMLInputElement).value}
                            onkeydown={handleKeydown}
                        />
                    </div>
                    <p class="text-muted text-sm">
                        A new user account will be created with server-generated keys.
                    </p>
                </div>
            {:else if step === 2}
                <div class="space-y-4">
                    <p class="text-muted text-sm">
                        Write this passphrase down and give it to <strong class="text-primary">{username}</strong>. It cannot be recovered if lost.
                    </p>
                    <PassphraseDisplayContent {passphrase} />
                </div>
            {:else}
                <div class="space-y-4">
                    <p class="text-muted text-sm">
                        Enter the following words from the passphrase to confirm you've written it down.
                    </p>
                    <PassphraseVerifyContent {passphrase} bind:this={verifyRef} />
                </div>
            {/if}
        {/snippet}

        {#snippet footer()}
            <div class="flex justify-end gap-2">
                <Button
                    icon="i-carbon-close"
                    text="Cancel"
                    onClick={handleClose}
                    disabled={loading}
                />
                {#if step === 1}
                    <Button
                        icon="i-carbon-add"
                        text="Create"
                        onClick={handleCreate}
                        disabled={loading || !username.trim()}
                    />
                {:else if step === 2}
                    <Button
                        icon="i-carbon-checkmark"
                        text="I've written it down"
                        onClick={handleWrittenDown}
                    />
                {:else}
                    <Button
                        icon="i-carbon-checkmark"
                        text="Verify"
                        onClick={handleVerify}
                    />
                {/if}
            </div>
        {/snippet}
    </Modal>
{/if}
