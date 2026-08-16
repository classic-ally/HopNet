<script lang="ts">
    import { formatDateForContainer } from '../utils/formatters'

    /**
     * A date cell that renders all three densities; the Table's container
     * measurement decides which span is visible (date-full / date-time /
     * date-only), so narrowing the pane trades detail for fit without any
     * per-cell JS. Expects an ISO string — unix timestamps must be converted
     * by the caller first.
     */
    let { date }: { date: string | null | undefined } = $props()

    const formats = $derived(date ? formatDateForContainer(date) : null)
</script>

{#if formats}
    <span class="date-full">{formats.full}</span>
    <span class="date-time">{formats.dateTime}</span>
    <span class="date-only">{formats.dateOnly}</span>
{:else}
    -
{/if}
