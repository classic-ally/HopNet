<script lang="ts">
  import { onMount } from 'svelte';
  import Setup from './lib/SetupSM.svelte';
  import LoginPane from './lib/LoginPane.svelte';
  import BackendError from './lib/BackendError.svelte';
  import { tokenStore, API_BASE_URL } from './lib/stores';
    import Interface from './lib/Interface/Interface.svelte';

  let currentComponent: 'setup' | 'operation' | 'error' = 'setup';
  let username = '';
  let password = '';

  // Reactive statement to get token value from store
  $: token = $tokenStore;

  // are we set up?
  onMount(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/setup`);
      
      if (response.status === 404) {
        currentComponent = 'setup';
      } else if (response.status === 200) {
        currentComponent = 'operation';
      } else {
        currentComponent = 'error';
      }
    } catch (error) {
      // Network error or other fetch failure
      currentComponent = 'error';
    }
  });
</script>

<main>
  <div class="flex justify-center items-center min-h-screen min-w-screen">
    {#if currentComponent === 'setup'}
      <Setup/>
    {:else if currentComponent === 'operation'}
      {#if token}
        <Interface />
      {:else}
        <LoginPane bind:username bind:password/>
      {/if}
    {:else if currentComponent === 'error'}
      <BackendError/>
    {/if}
  </div>
</main>
