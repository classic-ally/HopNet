<script lang="ts">
    import Button from '../../Button.svelte';
    import type { StepStatus } from './steps';

    interface Props {
        icon: string;
        title: string;
        summary: string;
        status: StepStatus;
        onClick: () => void;
    }

    let { icon, title, summary, status, onClick }: Props = $props();

    /// Status drives the trailing icon + colour. Encapsulates the mapping so
    /// callers don't repeat it; reuses Button's hover/focus/disabled chrome.
    const trailing = $derived({
        todo:   { icon: 'i-carbon-circle-dash',       cls: 'text-muted', text: 'Start' },
        active: { icon: 'i-carbon-circle-dash animate-spin', cls: 'text-blue', text: 'View progress' },
        done:   { icon: 'i-carbon-checkmark-filled',  cls: 'text-green', text: 'Review' },
    }[status]);
</script>

<Button
    variant="card"
    {icon}
    text={title}
    subtitle={summary}
    trailing={trailing.icon}
    trailingClass={trailing.cls}
    trailingText={trailing.text}
    {onClick}
/>
