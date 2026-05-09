<script lang="ts">
    import Modal from '../../primitives/Modal.svelte';
    import Button from '../../Button.svelte';
    import OnboardingChecklistItem from './OnboardingChecklistItem.svelte';
    import { STEPS, computeIncompleteSteps, type OnboardingStep } from './steps';
    import { refreshCurrentUser } from '../../stores';
    import type { ImportStatusState } from '../../stores';
    import { setOnboardingFlags } from '../../api/accounts';
    import type { OnboardingFlag, SelfUserInfo } from '../../types';

    interface Props {
        /// Current user. Pure prop so stories can mock without populating stores.
        user: SelfUserInfo;
        /// Live import substate.
        importState: ImportStatusState;
        /// If provided, modal opens directly to that step's page rather than
        /// the checklist view. Used by the import-progress banner to
        /// shortcut into the active import step.
        initialStepFlag?: string;
        onClose: () => void;
        /// Optional override for the dismiss-all submit. Stories pass a no-op.
        /// Prod calls real `setOnboardingFlags` + `refreshCurrentUser`.
        submitFlags?: (set: OnboardingFlag[], clear: OnboardingFlag[]) => Promise<void>;
    }

    let {
        user,
        importState,
        initialStepFlag = undefined,
        onClose,
        submitFlags = defaultSubmit,
    }: Props = $props();

    async function defaultSubmit(set: OnboardingFlag[], clear: OnboardingFlag[]) {
        await setOnboardingFlags(set, clear);
        await refreshCurrentUser();
    }

    /// `null` = checklist view; otherwise the active step's flag string.
    let activeFlag = $state<OnboardingFlag | null>(
        (initialStepFlag as OnboardingFlag | undefined) ?? null
    );
    let dismissError = $state<string | undefined>(undefined);
    let dismissing = $state(false);
    /// Two-stage confirmation for "Mark all as done" — prevents accidental
    /// click obliterating onboarding state. First click flips the footer
    /// into a Yes/Cancel pair; Yes commits, Cancel reverts.
    let confirmingMarkAll = $state(false);

    const items = $derived(STEPS.map(step => ({ step, status: step.statusOf(user, importState) })));
    const allDone = $derived(items.length > 0 && items.every(i => i.status === 'done'));
    const incomplete = $derived(computeIncompleteSteps(user, importState));

    const activeStep = $derived<OnboardingStep | null>(
        activeFlag ? STEPS.find(s => s.flag === activeFlag) ?? null : null
    );

    function openStep(flag: OnboardingFlag) { activeFlag = flag; }
    function backToChecklist() { activeFlag = null; }

    /// "Mark all as done" — bulk version of per-step Mark-as-done. Sets every
    /// still-incomplete bit at once so the checklist (and onboarding banner)
    /// disappear. Two-click confirmation guards against accidental dismissal.
    async function markAllAsDone() {
        if (incomplete.length === 0) {
            onClose();
            return;
        }
        dismissing = true;
        dismissError = undefined;
        try {
            await submitFlags(incomplete.map(s => s.flag), []);
            confirmingMarkAll = false;
            onClose();
        } catch (e) {
            dismissError = e instanceof Error ? e.message : 'Failed to update onboarding state';
        } finally {
            dismissing = false;
        }
    }
</script>

<Modal
    title={activeStep ? activeStep.title : (allDone ? 'You\'re all set' : 'Welcome to HopNet')}
    size="lg"
    {onClose}
    onBack={activeStep ? backToChecklist : undefined}
    loading={dismissing}
    error={dismissError}
    closeOnBackdrop={false}
>
    {#snippet content()}
        {#if activeStep}
            <activeStep.Component onBack={backToChecklist} />
        {:else if allDone}
            <div class="text-center py-6 space-y-3">
                <div class="i-carbon-checkmark-filled text-green text-5xl mx-auto"></div>
                <p class="text-primary">All onboarding steps are complete.</p>
                <p class="text-muted text-sm">You can close this dialog and start using HopNet.</p>
            </div>
        {:else}
            <p class="text-subtitle text-sm">
                A few quick steps to finish setting up your account. You can do them now or come back later.
            </p>
            <div class="space-y-2">
                {#each items as item}
                    <OnboardingChecklistItem
                        icon={item.step.icon}
                        title={item.step.title}
                        summary={item.step.summary}
                        status={item.status}
                        onClick={() => openStep(item.step.flag)}
                    />
                {/each}
            </div>
        {/if}
    {/snippet}

    {#snippet footer()}
        {#if activeStep}
            <!-- Footer left empty — the step's own buttons handle navigation. -->
            <div></div>
        {:else if allDone}
            <div class="flex justify-end">
                <Button icon="i-carbon-checkmark" text="Get started" onClick={onClose} />
            </div>
        {:else if confirmingMarkAll}
            <div class="flex justify-between items-center gap-3">
                <span class="text-xs text-yellow">Mark {incomplete.length} step{incomplete.length === 1 ? '' : 's'} as done without doing them?</span>
                <div class="flex gap-2">
                    <Button
                        icon="i-carbon-close"
                        text="Cancel"
                        onClick={() => { confirmingMarkAll = false; }}
                        disabled={dismissing}
                    />
                    <Button
                        icon="i-carbon-checkmark"
                        text="Yes, mark all as done"
                        onClick={markAllAsDone}
                        disabled={dismissing}
                    />
                </div>
            </div>
        {:else}
            <div class="flex justify-between items-center">
                <span class="text-xs text-muted">{incomplete.length} of {STEPS.length} remaining</span>
                <Button
                    icon="i-carbon-checkmark"
                    text="Mark all as done"
                    onClick={() => { confirmingMarkAll = true; }}
                    disabled={dismissing}
                />
            </div>
        {/if}
    {/snippet}
</Modal>
