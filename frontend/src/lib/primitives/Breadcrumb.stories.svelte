<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import Breadcrumb from './Breadcrumb.svelte';
  import Card from './Card.svelte';

  /** Build the trail for a folder path, the way BrowsePane does. */
  function crumbsFor(folder: string) {
    const segments = folder.split('/').filter(Boolean);
    let built = '';
    return [
      { label: 'Home', value: '/', href: '/browse', icon: 'i-carbon-home', iconOnly: true },
      ...segments.map((name) => {
        built += '/' + name;
        return { label: name, value: built, href: '/browse' + built };
      })
    ];
  }

  const ROOT = crumbsFor('/');
  const SHALLOW = crumbsFor('/photos/2026');
  const DEEP = crumbsFor('/photos/2026/summer/trip/iceland/day-three/raw');
  const LONG_NAMES = crumbsFor('/a-very-long-folder-name-that-keeps-going/another-extremely-long-directory-name/final');

  const { Story } = defineMeta({
    title: 'Primitives/Breadcrumb',
    component: Breadcrumb,
    parameters: {
      docs: {
        description: {
          component:
            'A path trail: nav > ol > li with presentational separators and aria-current on the ' +
            'leaf. Crumbs render as anchors when given an href, so middle-click and new-tab work, ' +
            'while a modified click is left to the browser. Omitting onNavigate yields a ' +
            'read-only display — the mode the upload and new-folder modals use. Past maxVisible ' +
            'segments the middle collapses behind a menu, so depth cannot shove the row.'
        }
      }
    }
  });
</script>

{#snippet rootOnly()}
  <div class="min-h-screen bg-crust p-6">
    <Breadcrumb segments={ROOT} onNavigate={(v) => console.log('navigate', v)} />
  </div>
{/snippet}

<Story name="Root Only" template={rootOnly} />

{#snippet shallow()}
  <div class="min-h-screen bg-crust p-6">
    <Breadcrumb segments={SHALLOW} onNavigate={(v) => console.log('navigate', v)} />
  </div>
{/snippet}

<Story name="Shallow" template={shallow} />

<!-- Eight segments, maxVisible 4: the middle collapses into the menu. -->
{#snippet deep()}
  <div class="min-h-screen bg-crust p-6">
    <Breadcrumb segments={DEEP} onNavigate={(v) => console.log('navigate', v)} />
  </div>
{/snippet}

<Story name="Deep (Collapsed)" template={deep} />

<!-- Read-only: no buttons, no anchors — what the modals render. -->
{#snippet readOnly()}
  <div class="min-h-screen bg-crust p-6">
    <Breadcrumb segments={SHALLOW} />
  </div>
{/snippet}

<Story name="Read Only" template={readOnly} />

<!-- Read-only and deep: the hidden trail lives in the ellipsis tooltip. -->
{#snippet readOnlyDeep()}
  <div class="min-h-screen bg-crust p-6">
    <Breadcrumb segments={DEEP} />
  </div>
{/snippet}

<Story name="Read Only Deep" template={readOnlyDeep} />

<!-- Long names truncate; the leaf keeps its space. -->
{#snippet longNames()}
  <div class="min-h-screen bg-crust p-6">
    <div class="max-w-lg border border-dashed border-overlay0 rounded p-2">
      <Breadcrumb segments={LONG_NAMES} onNavigate={(v) => console.log('navigate', v)} />
    </div>
  </div>
{/snippet}

<Story name="Long Segment Names" template={longNames} />

<!-- Narrow container: the row must not be pushed wider than its parent. -->
{#snippet narrow()}
  <div class="min-h-screen bg-crust p-6">
    <div class="max-w-xs border border-dashed border-overlay0 rounded p-2">
      <Breadcrumb segments={DEEP} onNavigate={(v) => console.log('navigate', v)} />
    </div>
  </div>
{/snippet}

<Story name="Narrow Container" template={narrow} />

<!-- In situ: inside a Card toolbar row, where the collapse menu has to escape
     the card's overflow-hidden. -->
{#snippet inToolbar()}
  <div class="min-h-screen bg-crust p-6">
    <Card padding={false}>
      <div class="flex items-center gap-2 p-2 border-b border-surface1">
        <Breadcrumb segments={DEEP} onNavigate={(v) => console.log('navigate', v)} />
      </div>
      <div class="p-4 text-sm text-muted">Table would render here.</div>
    </Card>
  </div>
{/snippet}

<Story name="In A Card Toolbar" template={inToolbar} />
