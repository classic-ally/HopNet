<script lang="ts">
  import Button from '../../Button.svelte';
  import { defaultFilterState, type FilterState, type TriState } from './filters';

  // Presentational: owns only the open/closed state; the filter value lives
  // in the page (App) so grid + histogram derive from one source.
  let {
    value,
    onChange,
  }: {
    value: FilterState;
    onChange: (next: FilterState) => void;
  } = $props();

  let open = $state(false);
  let root = $state<HTMLDivElement>();

  // favorites are Phase 4 — no sidecar column; re-add the favorite row when
  // the server grows the filter. FilterState keeps the field (pinned 'any').
  const TRI_ROWS: { key: 'live' | 'raw'; label: string; icon: string }[] = [
    { key: 'live', label: 'Live Photo', icon: 'i-carbon-renew' },
    { key: 'raw', label: 'RAW', icon: 'i-carbon-raw' },
  ];
  const TRI_OPTIONS: { v: TriState; label: string }[] = [
    { v: 'any', label: 'Any' },
    { v: 'only', label: 'Only' },
    { v: 'exclude', label: 'Not' },
  ];

  const activeCount = $derived(
    (value.photos && value.videos ? 0 : 1) +
      TRI_ROWS.filter((r) => value[r.key] !== 'any').length,
  );

  function toggleMedia(key: 'photos' | 'videos') {
    onChange({ ...value, [key]: !value[key] });
  }
  function setTri(key: 'live' | 'raw', v: TriState) {
    onChange({ ...value, [key]: v });
  }

  function onPointerDown(e: PointerEvent) {
    if (open && root && !root.contains(e.target as Node)) open = false;
  }
  function onKey(e: KeyboardEvent) {
    if (e.key === 'Escape') open = false;
  }
</script>

<svelte:document onpointerdown={onPointerDown} onkeydown={onKey} />

<div class="relative" bind:this={root}>
  <Button
    icon="i-carbon-filter"
    text={activeCount > 0 ? `Filter (${activeCount})` : 'Filter'}
    variant="desktop"
    className={activeCount > 0 || open ? 'bg-surface1 text-text' : 'text-subtitle'}
    onClick={() => (open = !open)}
  />

  {#if open}
    <div class="dropdown bg-mantle border border-overlay0/40" role="menu">
      <div class="section-label text-muted">Media</div>
      {#each [{ key: 'photos' as const, label: 'Photos', icon: 'i-carbon-image' }, { key: 'videos' as const, label: 'Videos', icon: 'i-carbon-video' }] as m (m.key)}
        <button class="row" role="menuitemcheckbox" aria-checked={value[m.key]} onclick={() => toggleMedia(m.key)}>
          <span class={value[m.key] ? 'i-carbon-checkmark text-mauve' : 'check-gap'}></span>
          <span class={`${m.icon} text-subtitle`}></span>
          <span class="flex-1 text-left">{m.label}</span>
        </button>
      {/each}

      <div class="divider bg-overlay0/40"></div>
      <div class="section-label text-muted">Refine</div>
      {#each TRI_ROWS as row (row.key)}
        <div class="row static-row">
          <span class="check-gap"></span>
          <span class={`${row.icon} text-subtitle`}></span>
          <span class="flex-1 text-left">{row.label}</span>
          <div class="segmented border border-overlay0/40">
            {#each TRI_OPTIONS as opt (opt.v)}
              <button
                class="seg"
                class:seg-active={value[row.key] === opt.v}
                aria-pressed={value[row.key] === opt.v}
                onclick={() => setTri(row.key, opt.v)}
              >
                {opt.label}
              </button>
            {/each}
          </div>
        </div>
      {/each}

      {#if activeCount > 0}
        <div class="divider bg-overlay0/40"></div>
        <button class="row text-subtitle" onclick={() => onChange({ ...defaultFilterState })}>
          <span class="i-carbon-reset check-gap"></span>
          <span class="flex-1 text-left">Reset filters</span>
        </button>
      {/if}
    </div>
  {/if}
</div>

<style>
  .dropdown {
    position: absolute;
    right: 0;
    top: calc(100% + 6px);
    z-index: 40;
    min-width: 17rem;
    border-radius: 0.5rem;
    padding: 0.375rem;
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45);
  }
  .section-label {
    padding: 0.25rem 0.5rem;
    font-size: 0.6875rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }
  .row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    width: 100%;
    padding: 0.375rem 0.5rem;
    border: none;
    border-radius: 0.375rem;
    background: none;
    color: #cdd6f4; /* text */
    font-size: 0.8125rem;
    cursor: pointer;
  }
  .row:not(.static-row):hover {
    background: #313244; /* surface0 */
  }
  .static-row {
    cursor: default;
  }
  .check-gap {
    width: 1em;
    height: 1em;
    flex-shrink: 0;
  }
  .divider {
    height: 1px;
    margin: 0.25rem 0.375rem;
  }
  .segmented {
    display: flex;
    border-radius: 0.375rem;
    overflow: hidden;
  }
  .seg {
    padding: 0.125rem 0.5rem;
    border: none;
    background: none;
    color: #a6adc8; /* subtext0 */
    font-size: 0.6875rem;
    cursor: pointer;
  }
  .seg + .seg {
    border-left: 1px solid rgba(108, 112, 134, 0.4); /* overlay0/40 */
  }
  .seg:hover {
    background: #313244;
  }
  .seg-active {
    background: #45475a; /* surface1 */
    color: #cba6f7; /* mauve */
  }
</style>
