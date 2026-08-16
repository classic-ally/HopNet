<script lang="ts">
    import { currentUserStore, refreshCurrentUser } from '../../stores';
    import { updateProfile } from '../../api/accounts';
    import Button from '../../Button.svelte';
    import Card from '../../primitives/Card.svelte';
    import TextInput from '../../primitives/TextInput.svelte';
    import AvatarCropModal from './AvatarCropModal.svelte';

    interface Props {
        /// Optional callback fired after a successful name save. Avatar
        /// uploads happen inside `AvatarCropModal` and trigger their own
        /// `refreshCurrentUser` — this callback is for consumers that need
        /// to react to the name-update specifically (AccountsPane refreshes
        /// the accounts table; the onboarding step flips its done flag).
        onSaved?: () => void;
    }

    let { onSaved }: Props = $props();

    let editFirstName = $state('');
    let editLastName = $state('');
    let profileSaving = $state(false);
    let profileSuccess = $state('');
    let profileError = $state('');
    let isAvatarModalOpen = $state(false);

    // Sync form fields to current user when the store updates (login,
    // refresh after save, cross-device flag flip).
    $effect(() => {
        if ($currentUserStore) {
            editFirstName = $currentUserStore.first_name || '';
            editLastName = $currentUserStore.last_name || '';
        }
    });

    const avatarSrc = $derived(
        $currentUserStore?.avatar ? `data:image/jpeg;base64,${$currentUserStore.avatar}` : null
    );

    async function handleProfileSave() {
        profileSaving = true;
        profileSuccess = '';
        profileError = '';
        try {
            const fields: { first_name?: string | null; last_name?: string | null } = {};
            const current = $currentUserStore;
            const newFirst = editFirstName.trim() || null;
            const newLast = editLastName.trim() || null;
            if (newFirst !== (current?.first_name || null)) fields.first_name = newFirst;
            if (newLast !== (current?.last_name || null)) fields.last_name = newLast;
            if (Object.keys(fields).length === 0) {
                profileSuccess = 'No changes';
                onSaved?.();
                return;
            }
            const response = await updateProfile(fields);
            if (!response.ok) throw new Error(`Failed: ${response.status}`);
            await refreshCurrentUser();
            profileSuccess = 'Profile updated';
            onSaved?.();
        } catch (err) {
            profileError = err instanceof Error ? err.message : 'Failed to update profile';
        } finally {
            profileSaving = false;
        }
    }
</script>

{#if $currentUserStore}
<Card title="My Profile" subtitle={$currentUserStore.username} icon="i-carbon-user-avatar">
    <div class="flex gap-4 items-start">
        <!-- One control, not two: the avatar and its caption both opened the
             crop modal, so they are a single button with a bigger hit area. -->
        <button
            type="button"
            class="flex flex-col items-center gap-1 flex-shrink-0 bg-transparent border-none p-0 cursor-pointer group
                   focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-mauve rounded"
            onclick={() => isAvatarModalOpen = true}
        >
            <span
                class="w-16 h-16 rounded-full overflow-hidden border-2 border-overlay1 bg-surface1
                       group-hover:border-mauve transition-colors flex items-center justify-center"
            >
                {#if avatarSrc}
                    <img src={avatarSrc} alt="" class="w-full h-full object-cover" />
                {:else}
                    <span class="i-carbon-user w-8 h-8 text-muted" aria-hidden="true"></span>
                {/if}
            </span>
            <span class="text-xs text-muted group-hover:text-primary transition-colors">Change</span>
        </button>
        <div class="flex-1 min-w-0 space-y-2">
            <!-- TextInput is w-full, so the halves are sized by these
                 wrappers rather than by fighting its own width class. -->
            <div class="flex gap-2">
                <div class="flex-1 min-w-0">
                    <TextInput
                        ariaLabel="First name"
                        placeholder="First name"
                        value={editFirstName}
                        maxlength={32}
                        disabled={profileSaving}
                        oninput={(e) => editFirstName = (e.target as HTMLInputElement).value}
                    />
                </div>
                <div class="flex-1 min-w-0">
                    <TextInput
                        ariaLabel="Last name"
                        placeholder="Last name"
                        value={editLastName}
                        maxlength={32}
                        disabled={profileSaving}
                        oninput={(e) => editLastName = (e.target as HTMLInputElement).value}
                    />
                </div>
            </div>
            <div class="flex gap-2 items-center">
                <Button
                    icon="i-carbon-save"
                    text={profileSaving ? 'Saving...' : 'Update'}
                    onClick={handleProfileSave}
                    disabled={profileSaving}
                />
                {#if profileSuccess}
                    <span class="text-sm text-green">{profileSuccess}</span>
                {/if}
                {#if profileError}
                    <span class="text-sm text-red">{profileError}</span>
                {/if}
            </div>
        </div>
    </div>
</Card>
{/if}

<AvatarCropModal
    isOpen={isAvatarModalOpen}
    onClose={() => isAvatarModalOpen = false}
/>
