<script lang="ts">
    import { currentUserStore, refreshCurrentUser } from '../../stores';
    import { updateProfile } from '../../api/accounts';
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
<div class="border-solid border-1 rounded-lg p-3 border-overlay1">
    <h3 class="mb-2 text-lg font-semibold">My Profile</h3>
    <div class="flex gap-4 items-start">
        <div class="flex flex-col items-center gap-1 flex-shrink-0">
            <button
                class="w-16 h-16 rounded-full overflow-hidden border-2 border-overlay1 hover:border-mauve transition-colors cursor-pointer bg-surface0 flex items-center justify-center"
                onclick={() => isAvatarModalOpen = true}
            >
                {#if avatarSrc}
                    <img src={avatarSrc} alt="Avatar" class="w-full h-full object-cover" />
                {:else}
                    <div class="i-carbon-user w-8 h-8 text-muted"></div>
                {/if}
            </button>
            <button
                class="text-xs text-muted hover:text-primary cursor-pointer bg-transparent border-none"
                onclick={() => isAvatarModalOpen = true}
            >Change</button>
        </div>
        <div class="flex-1 space-y-2">
            <div class="text-sm text-muted">{$currentUserStore.username}</div>
            <div class="flex gap-2">
                <input
                    class="flex-1 bg-transparent text-primary border-overlay0 border-2 border-solid rounded-md p-1 text-sm"
                    type="text"
                    placeholder="First name"
                    bind:value={editFirstName}
                    maxlength={32}
                    disabled={profileSaving}
                />
                <input
                    class="flex-1 bg-transparent text-primary border-overlay0 border-2 border-solid rounded-md p-1 text-sm"
                    type="text"
                    placeholder="Last name"
                    bind:value={editLastName}
                    maxlength={32}
                    disabled={profileSaving}
                />
            </div>
            <div class="flex gap-2 items-center">
                <button
                    class="text-sm px-2 py-1 rounded bg-surface0 border border-overlay1 text-primary hover:bg-overlay0 transition-colors disabled:opacity-50"
                    onclick={handleProfileSave}
                    disabled={profileSaving}
                >
                    {profileSaving ? 'Saving...' : 'Update'}
                </button>
                {#if profileSuccess}
                    <span class="text-sm text-green">{profileSuccess}</span>
                {/if}
                {#if profileError}
                    <span class="text-sm text-red">{profileError}</span>
                {/if}
            </div>
        </div>
    </div>
</div>
{/if}

<AvatarCropModal
    isOpen={isAvatarModalOpen}
    onClose={() => isAvatarModalOpen = false}
/>
