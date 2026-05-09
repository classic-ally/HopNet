// Onboarding step registry. Each entry describes one Welcome-modal item:
// what it does, when it counts as complete, and which page component to
// embed when the user opens it.
//
// Add a new step by:
//   1. Defining a new bit in `OnboardingFlag` (Rust + typeshare emit)
//   2. Implementing a step page component (sibling .svelte file)
//   3. Appending an entry to `STEPS` below
//
// The modal automatically picks up new entries; no orchestration changes
// needed.

import type { Component } from 'svelte';
import type { SelfUserInfo } from '../../types';
import { OnboardingFlag } from '../../types';
import type { ImportStatusState } from '../../stores';
import ImportStep from './ImportStep.svelte';

export type StepStatus = 'todo' | 'active' | 'done';

export interface OnboardingStep {
    /// Bit name in `OnboardingFlag` enum. Suppressing re-prompt is keyed on this.
    flag: OnboardingFlag;
    icon: string;
    title: string;
    summary: string;
    /// Page component embedded inside WelcomeModal when this step is active.
    Component: Component<{ onBack: () => void }>;
    /// Resolves to per-step status given current observed state. Allows steps
    /// like Import to expose 'active' (in-progress) distinct from 'done'.
    statusOf: (user: SelfUserInfo, importState: ImportStatusState) => StepStatus;
}

// Mirror the bit positions defined in `hopnet_common::OnboardingFlag::bit()`.
// Kept inline rather than fetched from backend because typeshare doesn't emit
// associated-const values.
const FLAG_BITS: Record<OnboardingFlag, number> = {
    [OnboardingFlag.ImportOffered]:   1 << 0,
    [OnboardingFlag.ImportCompleted]: 1 << 1,
};

export function flagBit(flag: OnboardingFlag): number {
    return FLAG_BITS[flag];
}

export function hasFlag(user: SelfUserInfo, flag: OnboardingFlag): boolean {
    return ((user.onboarding_flags ?? 0) & flagBit(flag)) !== 0;
}

export const STEPS: OnboardingStep[] = [
    {
        flag: OnboardingFlag.ImportOffered,
        icon: 'i-carbon-cloud-upload',
        title: 'Import existing data',
        summary: 'Bring in a HopNet takeout archive from a previous installation',
        Component: ImportStep,
        statusOf: (user, importState) => {
            const status = importState.record?.status;
            // Backend truth wins over the bit. If the post-Completed flag
            // setter raced with a network blip the bit may be unset even
            // though the import succeeded — surface as `done` regardless so
            // the user isn't re-prompted for an already-finished import.
            if (status === 'Completed' || status === 'Failed') return 'done';
            if (status === 'Pending' || status === 'Importing') return 'active';
            if (hasFlag(user, OnboardingFlag.ImportOffered)) return 'done';
            return 'todo';
        },
    },
];

/// Steps that haven't been done (or actively in-progress) for this user.
/// Used by the modal auto-open trigger and the banner count.
export function computeIncompleteSteps(user: SelfUserInfo, importState: ImportStatusState): OnboardingStep[] {
    return STEPS.filter(step => {
        const s = step.statusOf(user, importState);
        return s === 'todo' || s === 'active';
    });
}
