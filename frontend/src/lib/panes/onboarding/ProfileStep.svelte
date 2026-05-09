<script lang="ts">
    import { refreshCurrentUser } from '../../stores';
    import { setOnboardingFlags } from '../../api/accounts';
    import { OnboardingFlag } from '../../types';
    import Button from '../../Button.svelte';
    import ProfileEditor from '../accounts/ProfileEditor.svelte';

    interface Props { onBack: () => void; }
    let { onBack }: Props = $props();

    /// Flip the bit so the checklist marks the step done. Status is also
    /// derived from observed name/avatar fields, so users who fill the form
    /// would already show 'done' — but the bit lets users dismiss without
    /// filling in, and lets "Mark all as done" cover this step too.
    async function markDone() {
        try {
            await setOnboardingFlags([OnboardingFlag.ProfileCompleted], []);
            await refreshCurrentUser();
        } catch (_) { /* surface elsewhere */ }
    }

    async function handleSaved() {
        await markDone();
        onBack();
    }

    async function handleSkip() {
        await markDone();
        onBack();
    }
</script>

<div class="space-y-4">
    <p class="text-sm text-subtitle">
        Add a name and avatar so other people on your network can recognize you.
    </p>
    <ProfileEditor onSaved={handleSaved} />
    <div class="flex justify-end">
        <Button icon="i-carbon-checkmark" text="Skip for now" onClick={handleSkip} />
    </div>
</div>
