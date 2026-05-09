// Shared animation tokens. Three tiers based on the significance of the
// change being communicated — bigger context shifts get longer durations
// because the user needs more time to reorient.
//
// Spread directly into Svelte transition directives:
//   transition:fade={ANIM_PANE}
//   in:fly={{ ...ANIM_ROUTE, y: 8 }}

import { cubicOut } from 'svelte/easing';

export interface AnimSpec {
    duration: number;
    easing: (t: number) => number;
}

/// Small, local affordance changes (toast appear, focus highlight, badge
/// update). Should feel near-instant — present, not animated.
export const ANIM_MICRO: AnimSpec = { duration: 120, easing: cubicOut };

/// Pane swaps inside a single flow (setup wizard step, modal page,
/// onboarding step). Long enough to be perceived as a transition rather
/// than a flash, short enough that the user isn't waiting on the
/// animation to interact.
export const ANIM_PANE: AnimSpec = { duration: 200, easing: cubicOut };

/// Top-level app state changes (loading → setup/login/interface, login →
/// interface). Bigger reorient — chrome, layout, and information density
/// all change — so a longer fade gives the eye time to follow.
export const ANIM_ROUTE: AnimSpec = { duration: 300, easing: cubicOut };
