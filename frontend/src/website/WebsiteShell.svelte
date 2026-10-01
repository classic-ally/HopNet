<script lang="ts">
    // The real app chrome on a fake backend, routed to one pane. Setup runs in
    // the script body (before Interface mounts) so the first render already
    // sees the mock and the target path.
    import { onDestroy } from 'svelte';
    import Interface from '../lib/Interface/Interface.svelte';
    import { router } from '../lib/router.svelte';
    import { installMockBackend, type MockRoute } from './mockBackend';
    import { ME, shellRoutes } from './fixtures';

    let { path, routes }: { path: string; routes: MockRoute[] } = $props();

    // Keep the query string: in Storybook it carries the story id.
    const original = window.location.pathname + window.location.search;
    const restore = installMockBackend([...shellRoutes, ...routes], ME.user_id);
    router.replace(path + window.location.search);

    onDestroy(() => {
        restore();
        router.replace(original);
    });
</script>

<Interface />
