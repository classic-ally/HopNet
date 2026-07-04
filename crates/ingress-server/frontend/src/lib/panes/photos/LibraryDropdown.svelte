<script lang="ts">
  import Button from '$ui/Button.svelte';
  import type { LibrarySummary } from '../../types';

  // Library selector: check one or many; checked libraries fuse into a single
  // timeline. Deliberately separate from FilterDropdown — "where am I looking"
  // vs "what am I looking for".
  let {
    libraries,
    selected,
    onChange,
  }: {
    libraries: LibrarySummary[];
    /** Selected library_ids; always at least one. */
    selected: string[];
    onChange: (next: string[]) => void;
  } = $props();

  let open = $state(false);
  let root = $state<HTMLDivElement>();

  const label = $derived.by(() => {
    if (selected.length === libraries.length && libraries.length > 1) return 'All libraries';
    if (selected.length === 1) {
      return libraries.find((l) => l.library_id === selected[0])?.display_name ?? '1 library';
    }
    return `${selected.length} libraries`;
  });

  const nf = new Intl.NumberFormat();

  function toggle(id: string) {
    if (selected.includes(id)) {
      // Never allow an empty selection — the grid needs somewhere to look.
      if (selected.length > 1) onChange(selected.filter((s) => s !== id));
    } else {
      // Preserve config order so the label/fusion stays stable.
      onChange(libraries.map((l) => l.library_id).filter((l) => selected.includes(l) || l === id));
    }
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
    icon="i-carbon-folder"
    text={label}
    variant="desktop"
    className={open ? 'bg-surface1 text-text' : 'text-subtitle'}
    onClick={() => (open = !open)}
  />

  {#if open}
    <div class="dropdown bg-mantle border border-overlay0/40" role="menu">
      {#each libraries as lib (lib.library_id)}
        {@const on = selected.includes(lib.library_id)}
        <button
          class="row"
          role="menuitemcheckbox"
          aria-checked={on}
          onclick={() => toggle(lib.library_id)}
        >
          <span class={on ? 'i-carbon-checkmark text-mauve' : 'check-gap'}></span>
          <span
            class={`${lib.shared ? 'i-carbon-user-multiple' : 'i-carbon-folder'} text-subtitle`}
          ></span>
          <span class="name">
            <span class="text-left">{lib.display_name}</span>
            <span class="counts text-muted">
              <!-- ?? 0: tolerate a not-yet-redeployed backend that predates
                   the photo/video breakdown. -->
              {nf.format(lib.photo_count ?? 0)} photos · {nf.format(lib.video_count ?? 0)} videos
            </span>
          </span>
        </button>
      {/each}
    </div>
  {/if}
</div>

<style>
  .dropdown {
    position: absolute;
    left: 0;
    top: calc(100% + 6px);
    z-index: 40;
    min-width: 16rem;
    border-radius: 0.5rem;
    padding: 0.375rem;
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45);
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
  .row:hover {
    background: #313244; /* surface0 */
  }
  .check-gap {
    width: 1em;
    height: 1em;
    flex-shrink: 0;
  }
  .name {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    line-height: 1.25;
  }
  .counts {
    font-size: 0.6875rem;
  }
</style>
