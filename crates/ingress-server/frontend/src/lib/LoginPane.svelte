<script lang="ts">
  import Button from '$ui/Button.svelte';
  import SetupPane from '$ui/SetupPane.svelte';

  // Stub login page: HopNet's SetupPane card (logo + copy + actions), but the
  // only action is handing the browser to the OIDC provider — the viewer is a
  // BFF with a server-side session, there's nothing to type here. At HopNet
  // fold-in this dissolves into the real LoginPane.
  let {
    onLogin,
    reason,
  }: {
    onLogin: () => void;
    /** Optional context line, e.g. session-expired vs first visit. */
    reason?: string;
  } = $props();
</script>

<div class="fixed inset-0 z-50 flex items-center justify-center bg-crust">
  <SetupPane
    logoSrc="/hopnet-logo.png"
    title="Photos"
    body={reason ?? 'Sign in to browse your photo libraries.'}
    buttonsClass="flex justify-center"
  >
    {#snippet features()}
      <div class="flex items-center gap-2 text-subtitle text-sm px-1">
        <span class="i-carbon-password shrink-0"></span>
        <span>Authentication is handled by your identity provider.</span>
      </div>
    {/snippet}
    {#snippet buttons()}
      <Button icon="i-carbon-login" text="Sign in" variant="desktop" onClick={onLogin} />
    {/snippet}
  </SetupPane>
</div>
