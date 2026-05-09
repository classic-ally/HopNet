<script lang="ts">
  import { onMount } from 'svelte';
  import { fade } from 'svelte/transition';
  import Setup from './lib/panes/setup/SetupSM.svelte';
  import LoginPane from './lib/panes/setup/LoginPane.svelte';
  import BackendError from './lib/BackendError.svelte';
  import StatusSpinner from './lib/primitives/StatusSpinner.svelte';
  import { mergeStatusWords } from './lib/primitives/statusWords';
  import { ANIM_PANE, ANIM_ROUTE } from './lib/primitives/animation';
  import { tokenStore, API_BASE_URL } from './lib/stores';
  import Interface from './lib/Interface/Interface.svelte';

  // 'loading' suppresses any chrome until the /setup probe resolves so the
  // user doesn't see a flash of the Welcome page on fast backends. A
  // separate splash is shown only if the probe takes longer than
  // SPLASH_AFTER_MS — keeps fast-path renders silent.
  let currentComponent: 'loading' | 'setup' | 'operation' | 'error' = 'loading';
  let showSplash = false;
  let username = '';
  let passphrase = '';

  // Reactive statement to get token value from store
  $: token = $tokenStore;

  const SPLASH_AFTER_MS = 250;
  // Hard ceiling for the probe. Closed-port fetches fail fast (browser
  // raises within ms), but a half-open backend that accepts and never
  // responds would hang the splash forever otherwise. After this we route
  // to BackendError so the user has a recoverable view.
  const PROBE_TIMEOUT_MS = 8000;
  const SPLASH_WORDS = mergeStatusWords();

  // are we set up?
  onMount(async () => {
    const splashTimer = setTimeout(() => { showSplash = true; }, SPLASH_AFTER_MS);
    const controller = new AbortController();
    const probeTimer = setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS);

    try {
      const response = await fetch(`${API_BASE_URL}/setup`, { signal: controller.signal });

      if (response.status === 404) {
        currentComponent = 'setup';
      } else if (response.status === 200) {
        // Try Tauri IPC auto-login (GUI mode only)
        if ((window as any).__TAURI__) {
          try {
            const { invoke } = await import('@tauri-apps/api/core');
            const data: { token: string } = await invoke('auto_login');
            tokenStore.set(data.token);
          } catch (e) { /* auto-login not available, show login pane */ }
        }
        currentComponent = 'operation';
      } else {
        currentComponent = 'error';
      }
    } catch (error) {
      // Network error, abort, or other fetch failure.
      currentComponent = 'error';
    } finally {
      clearTimeout(splashTimer);
      clearTimeout(probeTimer);
      showSplash = false;
    }
  });
</script>

<main>
  <div class="flex justify-center items-center min-h-screen min-w-screen">
    {#if currentComponent === 'loading'}
      {#if showSplash}
        <div in:fade={ANIM_PANE} class="flex flex-col items-center gap-4">
          <img src="/hopnet-logo.png" alt="HopNet" class="w-40 h-auto" />
          <StatusSpinner words={SPLASH_WORDS} />
        </div>
      {/if}
    {:else if currentComponent === 'setup'}
      <div in:fade={ANIM_ROUTE}><Setup/></div>
    {:else if currentComponent === 'operation'}
      {#if token}
        <div in:fade={ANIM_ROUTE}><Interface /></div>
      {:else}
        <div in:fade={ANIM_ROUTE}><LoginPane bind:username bind:passphrase/></div>
      {/if}
    {:else if currentComponent === 'error'}
      <div in:fade={ANIM_ROUTE}><BackendError/></div>
    {/if}
  </div>
</main>
