<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import LoginPane from './LoginPane.svelte';

  const { Story } = defineMeta({
    title: 'Photos/LoginPane',
    component: LoginPane,
  });

  const onLogin = () => console.log('login → /auth/login');
</script>

<!-- First visit: logo card + provider hand-off button. -->
<Story name="Signed out">
  {#snippet template()}
    <div style="height: 100vh; position: relative;">
      <LoginPane {onLogin} />
    </div>
  {/snippet}
</Story>

<!-- Mid-session 401: same card with the expiry explanation. -->
<Story name="Session expired">
  {#snippet template()}
    <div style="height: 100vh; position: relative;">
      <LoginPane {onLogin} reason="Your session has expired — sign in again to continue." />
    </div>
  {/snippet}
</Story>
