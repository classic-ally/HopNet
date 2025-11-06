<script lang="ts">
    import Button from "../../Button.svelte";
    import Modal from "../../primitives/Modal.svelte";
    import TextInput from "../../primitives/TextInput.svelte";
    import { tokenStore, API_BASE_URL, authenticatedFetch } from '../../stores';

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
    let ip = $state('');
    let port = $state('');
    let publicKey = $state('');
    let isAdding = $state(false);
    let addError = $state('');

    // Helper to decode JWT and get user ID
    function getUserIdFromToken(): number | null {
        const token = $tokenStore;
        if (!token) return null;

        try {
            const base64Url = token.split('.')[1];
            const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
            const jsonPayload = decodeURIComponent(
                atob(base64).split('').map(function (c) {
                    return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
                }).join('')
            );
            const payload = JSON.parse(jsonPayload);
            return parseInt(payload.uid);
        } catch (e) {
            console.error('Failed to decode JWT', e);
            return null;
        }
    }

    function validateInputs(): boolean {
        if (!name.trim()) return false;
        if (!ip.trim()) return false;
        if (!port.trim()) return false;
        if (!publicKey.trim()) return false;

        // Basic IP validation (could be enhanced)
        const ipPattern = /^(\d{1,3}\.){3}\d{1,3}$/;
        if (!ipPattern.test(ip)) return false;

        // Port validation
        const portNum = parseInt(port);
        if (isNaN(portNum) || portNum < 1 || portNum > 65535) return false;

        // Public key validation (should be hex string)
        if (!/^[0-9a-fA-F]+$/.test(publicKey.trim())) return false;

        return true;
    }

    function handleClose() {
        name = '';
        ip = '';
        port = '';
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

        const userId = getUserIdFromToken();
        if (!userId) {
            const error = 'Unable to get user ID from token';
            addError = error;
            onError(error);
            return;
        }

        isAdding = true;
        addError = '';

        try {
            const nodeData = {
                node_id: 0, // Will be assigned by backend
                name: name.trim(),
                ip_address: ip.trim(),
                port: parseInt(port),
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

        <!-- IP Address Input -->
        <div class="space-y-2">
            <div class="block text-sm font-medium text-subtitle">
                IP Address
            </div>
            <TextInput
                value={ip}
                placeholder="e.g., 192.168.1.100"
                disabled={isAdding}
                onkeydown={handleKeydown}
                oninput={(e: Event) => ip = (e.target as HTMLInputElement).value}
            />
        </div>

        <!-- Port Input -->
        <div class="space-y-2">
            <div class="block text-sm font-medium text-subtitle">
                Port
            </div>
            <TextInput
                value={port}
                placeholder="e.g., 34633"
                disabled={isAdding}
                onkeydown={handleKeydown}
                oninput={(e: Event) => port = (e.target as HTMLInputElement).value}
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