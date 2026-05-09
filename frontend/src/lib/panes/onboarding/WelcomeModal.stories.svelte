<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import WelcomeModal from './WelcomeModal.svelte';
    import { OnboardingFlag } from '../../types';
    import type { SelfUserInfo } from '../../types';
    import type { ImportStatusState } from '../../stores';
    import { flagBit } from './steps';

    const { Story } = defineMeta({
        title: 'Panes/Onboarding/WelcomeModal',
        component: WelcomeModal,
        parameters: {
            docs: { description: { component: 'Welcome / onboarding modal. Shown automatically post-login when any onboarding step is incomplete; reopened from the onboarding banner. Stories pass mock user + import state directly so the orchestrator can be exercised without a backend.' } },
        },
    });

    const baseUser: SelfUserInfo = {
        user_id: 1,
        username: 'allison',
        first_name: undefined,
        last_name: undefined,
        avatar: undefined,
        onboarding_flags: 0,
    };

    const idleImport: ImportStatusState = { record: null, counts: null, loading: false };
    const importingState: ImportStatusState = {
        record: { id: 'fake-uuid', user_id: 1, owner_node_id: 1, status: 'Importing' as any, created_at: '' },
        counts: { total: 15, pending: 12, imported: 3, skipped: 0, failed: 0 },
        loading: false,
    };
    const completedState: ImportStatusState = {
        record: { id: 'fake-uuid', user_id: 1, owner_node_id: 1, status: 'Completed' as any, created_at: '' },
        counts: { total: 15, pending: 0, imported: 15, skipped: 0, failed: 0 },
        loading: false,
    };

    const noopSubmit = async (_set: OnboardingFlag[], _clear: OnboardingFlag[]) => { console.log('mock submitFlags'); };
</script>

<Story name="Checklist — fresh user (todo)" args={{
    user: baseUser,
    importState: idleImport,
    onClose: () => console.log('close'),
    submitFlags: noopSubmit,
}}>
    {#snippet template(args)}
        <div class="bg-base min-h-screen relative">
            <WelcomeModal {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Checklist — import in progress (active)" args={{
    user: baseUser,
    importState: importingState,
    onClose: () => console.log('close'),
    submitFlags: noopSubmit,
}}>
    {#snippet template(args)}
        <div class="bg-base min-h-screen relative">
            <WelcomeModal {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Checklist — all done" args={{
    user: { ...baseUser, onboarding_flags: flagBit(OnboardingFlag.ImportOffered) | flagBit(OnboardingFlag.ImportCompleted) },
    importState: completedState,
    onClose: () => console.log('close'),
    submitFlags: noopSubmit,
}}>
    {#snippet template(args)}
        <div class="bg-base min-h-screen relative">
            <WelcomeModal {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Step page — Import open at idle" args={{
    user: baseUser,
    importState: idleImport,
    initialStepFlag: 'ImportOffered',
    onClose: () => console.log('close'),
    submitFlags: noopSubmit,
}}>
    {#snippet template(args)}
        <div class="bg-base min-h-screen relative">
            <WelcomeModal {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Step page — Import open at progress" args={{
    user: baseUser,
    importState: importingState,
    initialStepFlag: 'ImportOffered',
    onClose: () => console.log('close'),
    submitFlags: noopSubmit,
}}>
    {#snippet template(args)}
        <div class="bg-base min-h-screen relative">
            <WelcomeModal {...args} />
        </div>
    {/snippet}
</Story>
