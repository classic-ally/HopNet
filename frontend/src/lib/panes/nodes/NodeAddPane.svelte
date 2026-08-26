<script lang="ts">
    import Button from "../../Button.svelte";
    import Modal from "../../primitives/Modal.svelte";
    import TextInput from "../../primitives/TextInput.svelte";
    import { API_BASE_URL, authenticatedFetch, getCurrentUserId } from '../../stores';

    // Props for external control
    interface NodeAddProps {
        isOpen?: boolean;
        onClose?: () => void;
        onNodeAdded?: () => void;
        onError?: (error: string) => void;
    }

    let {
        isOpen = false,
        onClose = () => {},
        onNodeAdded = () => {},
        onError = () => {}
    }: NodeAddProps = $props();

    let name = $state('');
    let publicKey = $state('');
    let isAdding = $state(false);
    let addError = $state('');
    // The mesh code (RFC-025 S5): read on modal open, shown so the
    // operator can enter it on the joining device — a fresh device is
    // unreachable until it adopts this code.
    let meshCode = $state<string | null>(null);

    $effect(() => {
        if (!isOpen) return;
        authenticatedFetch(`${API_BASE_URL}/views/regenesis-status`)
            .then((r) => (r.ok ? r.json() : null))
            .then((view) => {
                meshCode = view?.mesh_code ?? null;
            })
            .catch(() => {
                meshCode = null;
            });
    });

    function validateInputs(): boolean {
        if (!name.trim()) return false;
        if (!publicKey.trim()) return false;

        // Public key validation (should be hex string)
        if (!/^[0-9a-fA-F]+$/.test(publicKey.trim())) return false;

        return true;
    }

    function handleClose() {
        name = '';
        publicKey = '';
        addError = '';
        onClose();
    }

    async function addNode() {
        if (!validateInputs()) {
            const error = 'Please fill in all fields with valid data';
            addError = error;
            onError(error);
            return;
        }

        const userId = getCurrentUserId();
        if (userId === null) {
            const error = 'Unable to get user ID from token';
            addError = error;
            onError(error);
            return;
        }

        isAdding = true;
        addError = '';

        try {
            const nodeData = {
                name: name.trim(),
                owner: userId,
                pubkey: publicKey.trim()
            };

            const response = await authenticatedFetch(`${API_BASE_URL}/nodes`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(nodeData),
            });

            if (response.ok) {
                onNodeAdded();
                handleClose();
            } else {
                const errorText = await response.text().catch(() => 'Unknown error');
                const error = `Failed to add node: ${response.status} ${response.statusText} - ${errorText}`;
                addError = error;
                onError(error);
            }
        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : 'Failed to add node';
            addError = errorMessage;
            onError(errorMessage);
        } finally {
            isAdding = false;
        }
    }

    // Handle Enter key in text input
    function handleKeydown(event: KeyboardEvent) {
        if (event.key === 'Enter' && !isAdding && validateInputs()) {
            addNode();
        }
    }
</script>

{#if isOpen}
<Modal title="Add a Node" onClose={handleClose} size="md">
    {#snippet content()}
        <!-- Mesh code: the joining device needs this FIRST — it is
             unreachable until the code is entered there. -->
        {#if meshCode}
            <div class="space-y-1">
                <div class="block text-sm font-medium text-subtitle">
                    Mesh code
                </div>
                <div class="font-mono text-2xl tracking-widest text-primary">{meshCode}</div>
                <p class="text-muted text-sm">
                    Enter this code on the joining device's Join Network screen
                    before adding it here.
                </p>
            </div>
        {/if}

        <!-- Name Input -->
        <div class="space-y-2">
            <div class="block text-sm font-medium text-subtitle">
                Name
            </div>
            <TextInput
                value={name}
                placeholder="e.g., Alice's Laptop"
                disabled={isAdding}
                onkeydown={handleKeydown}
                oninput={(e: Event) => name = (e.target as HTMLInputElement).value}
            />
        </div>

        <!-- Public Key Input -->
        <div class="space-y-2">
            <div class="block text-sm font-medium text-subtitle">
                Public Key (hex)
            </div>
            <textarea
                bind:value={publicKey}
                placeholder="Enter public key in hex format"
                disabled={isAdding}
                onkeydown={handleKeydown}
                class="w-full box-border px-3 py-2 bg-base border border-overlay1 rounded-lg text-primary placeholder-muted focus:outline-none focus:ring-2 focus:ring-mauve focus:border-transparent transition-all resize-none font-mono text-sm"
                rows="3"
            ></textarea>
        </div>

        <!-- Error Message -->
        {#if addError}
            <div class="bg-red/20 border border-red rounded p-3">
                <p class="text-red text-sm">{addError}</p>
            </div>
        {/if}

        <!-- Adding Indicator -->
        {#if isAdding}
            <div class="bg-mauve/20 border border-mauve rounded p-3">
                <p class="text-mauve text-sm">Adding node...</p>
            </div>
        {/if}
    {/snippet}

    {#snippet footer()}
        <div class="flex items-center justify-end gap-2">
            <Button
                icon="i-carbon-close"
                text="Cancel"
                onClick={handleClose}
                disabled={isAdding}
            />
            <Button
                icon="i-carbon-add"
                text={isAdding ? 'Adding...' : 'Add Node'}
                onClick={addNode}
                disabled={!validateInputs() || isAdding}
            />
        </div>
    {/snippet}
</Modal>
{/if}
