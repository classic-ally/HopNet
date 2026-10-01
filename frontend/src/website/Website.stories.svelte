<script module lang="ts">
  // Screenshots for hopnet.app, captured by scripts/website-shots.ts. Each
  // story is the real Interface on curated fixture data; story names map to the
  // website's screenshot slots there.
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import { expect, userEvent, waitFor, within } from 'storybook/test';
  import WebsiteShell from './WebsiteShell.svelte';
  import { SHARE_TARGET, filesRoutes, photosRoutes, takeoutRoutes } from './fixtures';

  const { Story } = defineMeta({
    title: 'Website',
    parameters: { layout: 'fullscreen', backgrounds: { default: 'dark' } },
  });
</script>

<Story name="Photos gallery">
  {#snippet template()}
    <WebsiteShell path="/photos" routes={photosRoutes} />
  {/snippet}
</Story>

<Story name="Files">
  {#snippet template()}
    <WebsiteShell path="/browse" routes={filesRoutes} />
  {/snippet}
</Story>

<!-- Select a file and open the real share dialog over the browse pane. -->
<Story
  name="Sharing"
  play={async ({ canvasElement }) => {
    const canvas = within(canvasElement.ownerDocument.body);
    await userEvent.click(await canvas.findByText(SHARE_TARGET));
    await userEvent.click(await canvas.findByRole('button', { name: 'Share selected files' }));
    await userEvent.click(await canvas.findByRole('button', { name: 'priya' }));
    await waitFor(() => expect(canvas.getByText(/with:/)).toBeVisible());
  }}
>
  {#snippet template()}
    <WebsiteShell path="/browse" routes={filesRoutes} />
  {/snippet}
</Story>

<Story name="Export">
  {#snippet template()}
    <WebsiteShell path="/settings/takeout" routes={takeoutRoutes} />
  {/snippet}
</Story>
