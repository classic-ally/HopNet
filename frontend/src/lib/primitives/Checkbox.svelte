<script lang="ts">
    import type { Snippet } from 'svelte';

    /**
     * A checkbox, shaped after shadcn's: a small square that fills with the
     * accent colour and shows a tick when checked, a dash when indeterminate.
     *
     * Built on a real `<input type="checkbox">` kept in the accessibility tree
     * but visually hidden, with the square drawn beside it. That is deliberate
     * rather than a `role="checkbox"` div: space-to-toggle, form participation,
     * the label association and the announced checked state all come from the
     * platform instead of being re-implemented. The visible square is
     * `aria-hidden` so it is never announced twice.
     *
     * Both control styles are supported, because the codebase already uses each:
     * `bind:checked` for a preference toggle, or `checked` plus
     * `onCheckedChange` for a row whose selection lives in a table's state.
     */
    interface CheckboxProps {
        checked?: boolean;
        /**
         * Neither checked nor unchecked — for a select-all that covers only
         * some rows. Takes visual precedence over `checked`, matching the
         * platform, where the DOM property behaves the same way.
         */
        indeterminate?: boolean;
        disabled?: boolean;
        /// Marks the field as failing validation; surfaced as aria-invalid.
        invalid?: boolean;
        /// Convenience for the common case of a plain text label.
        label?: string;
        /// Richer label content, when a string will not do.
        children?: Snippet;
        /**
         * Set when the visible label lives elsewhere, so the control is still
         * named — a checkbox in a table cell being the usual case.
         */
        ariaLabel?: string;
        name?: string;
        value?: string;
        onCheckedChange?: (checked: boolean) => void;
        /// Applied to the wrapping label, for a caller's own colour or spacing.
        className?: string;
    }

    let {
        checked = $bindable(false),
        indeterminate = $bindable(false),
        disabled = false,
        invalid = false,
        label = undefined,
        children = undefined,
        ariaLabel = undefined,
        name = undefined,
        value = undefined,
        onCheckedChange = undefined,
        className = '',
    }: CheckboxProps = $props();

    // Ties the label to the input without the caller having to invent an id,
    // and without two checkboxes on one page colliding.
    const id = $props.id();

    const hasLabel = $derived(Boolean(label || children));

    const boxState = $derived(
        indeterminate || checked
            ? invalid
                ? 'bg-red border-red text-crust'
                : 'bg-mauve border-mauve text-crust'
            : invalid
              ? 'bg-transparent border-red'
              : 'bg-transparent border-overlay1'
    );
</script>

<label
    for={id}
    class="inline-flex items-center gap-2 {disabled
        ? 'cursor-not-allowed opacity-50'
        : 'cursor-pointer'} select-none {className}"
>
    <input
        {id}
        {name}
        {value}
        {disabled}
        type="checkbox"
        bind:checked
        bind:indeterminate
        aria-invalid={invalid || undefined}
        aria-label={hasLabel ? undefined : ariaLabel}
        onchange={(event) => onCheckedChange?.(event.currentTarget.checked)}
        class="peer sr-only"
    />

    <span
        aria-hidden="true"
        class="size-4 shrink-0 rounded-sm border border-solid grid place-items-center transition-colors {boxState}
               peer-focus-visible:outline peer-focus-visible:outline-2 peer-focus-visible:outline-offset-2 peer-focus-visible:outline-mauve"
    >
        {#if indeterminate}
            <div class="i-carbon-subtract text-xs"></div>
        {:else if checked}
            <div class="i-carbon-checkmark text-xs"></div>
        {/if}
    </span>

    {#if label}
        <span class="text-sm">{label}</span>
    {:else if children}
        {@render children()}
    {/if}
</label>
