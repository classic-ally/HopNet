<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import DropdownMenu from './DropdownMenu.svelte';
  import Card from './Card.svelte';

  const SIMPLE = [
    { label: 'Rename', icon: 'i-carbon-edit' },
    { label: 'Duplicate', icon: 'i-carbon-copy' },
    { label: 'Move to…', icon: 'i-carbon-folder-move-to' }
  ];

  const WITH_DESTRUCTIVE = [
    { label: 'Open', icon: 'i-carbon-launch' },
    { label: 'Download', icon: 'i-carbon-cloud-download' },
    { label: 'Share', icon: 'i-carbon-share' },
    { label: 'Delete', icon: 'i-carbon-trash-can', destructive: true, separatorBefore: true }
  ];

  const WITH_DISABLED = [
    { label: 'Open', icon: 'i-carbon-launch' },
    { label: 'Download', icon: 'i-carbon-cloud-download', disabled: true },
    { label: 'Share', icon: 'i-carbon-share' },
    { label: 'Revoke', icon: 'i-carbon-close', disabled: true, destructive: true }
  ];

  // Hidden breadcrumb ancestors — the shape the collapse menu actually uses.
  const FOLDERS = [
    { label: 'photos', href: '/browse/photos' },
    { label: '2026', href: '/browse/photos/2026' },
    { label: 'trip', href: '/browse/photos/2026/trip' }
  ];

  const MANY = Array.from({ length: 14 }, (_, i) => ({
    label: `folder-${String(i).padStart(2, '0')}`,
    icon: 'i-carbon-folder'
  }));

  const { Story } = defineMeta({
    title: 'Primitives/DropdownMenu',
    component: DropdownMenu,
    parameters: {
      docs: {
        description: {
          component:
            'A menu anchored to a trigger. The panel is portalled to <body> and fixed-positioned ' +
            'from the trigger rect, so it escapes Card’s overflow-hidden rather than being ' +
            'clipped by it. Items are data: role="menu" with role="menuitem" children, arrow-key ' +
            'navigation that skips disabled items, Escape closing and restoring focus to the ' +
            'trigger, and outside-pointer dismissal.'
        }
      }
    }
  });
</script>

{#snippet plainTrigger(ctx)}
  <button
    type="button"
    class="flex items-center gap-1 text-primary bg-surface1 border border-overlay1 rounded-md px-3 py-1.5 text-sm cursor-pointer"
    {...ctx.props}
  >
    Actions
    <span class="i-carbon-chevron-down w-4 h-4"></span>
  </button>
{/snippet}

{#snippet ellipsisTrigger(ctx)}
  <button
    type="button"
    class="text-muted hover:text-primary bg-transparent border-none cursor-pointer px-1"
    aria-label="Show hidden folders"
    {...ctx.props}
  >
    <span class="i-carbon-overflow-menu-horizontal w-4 h-4 block"></span>
  </button>
{/snippet}

{#snippet closed()}
  <div class="min-h-screen bg-crust p-6">
    <DropdownMenu items={SIMPLE} trigger={plainTrigger} ariaLabel="Row actions" />
  </div>
{/snippet}

<Story name="Closed" template={closed} />

<!-- open initially, so screenshots need no interaction -->
{#snippet openMenu()}
  <div class="min-h-screen bg-crust p-6">
    <DropdownMenu items={SIMPLE} trigger={plainTrigger} ariaLabel="Row actions" open />
  </div>
{/snippet}

<Story name="Open" template={openMenu} />

{#snippet destructive()}
  <div class="min-h-screen bg-crust p-6">
    <DropdownMenu items={WITH_DESTRUCTIVE} trigger={plainTrigger} ariaLabel="File actions" open />
  </div>
{/snippet}

<Story name="Separator And Destructive" template={destructive} />

{#snippet disabled()}
  <div class="min-h-screen bg-crust p-6">
    <DropdownMenu items={WITH_DISABLED} trigger={plainTrigger} ariaLabel="File actions" open />
  </div>
{/snippet}

<Story name="Disabled Items" template={disabled} />

<!-- Right-aligned: the panel's right edge meets the trigger's. -->
{#snippet alignEnd()}
  <div class="min-h-screen bg-crust p-6 flex justify-end">
    <DropdownMenu items={SIMPLE} trigger={plainTrigger} ariaLabel="Row actions" align="end" open />
  </div>
{/snippet}

<Story name="Align End" template={alignEnd} />

<!--
  The story that matters: a trigger inside a Card, which carries
  overflow-hidden. An absolutely positioned panel would be clipped here; the
  portalled one is not.
-->
{#snippet insideCard()}
  <div class="min-h-screen bg-crust p-6">
    <Card padding={false}>
      <div class="flex items-center gap-2 p-2 border-b border-surface1">
        <span class="text-subtitle text-sm font-mono">/ photos</span>
        <DropdownMenu items={FOLDERS} trigger={ellipsisTrigger} ariaLabel="Show 3 hidden folders" open />
      </div>
      <div class="p-4 text-sm text-muted">Card content below the toolbar row.</div>
    </Card>
  </div>
{/snippet}

<Story name="Inside A Card (Portal Escapes Clipping)" template={insideCard} />

<!-- Trigger near the viewport bottom: the panel flips above it. -->
{#snippet flipUp()}
  <div class="min-h-screen bg-crust p-6 flex flex-col justify-end">
    <div class="h-[85vh]"></div>
    <DropdownMenu items={SIMPLE} trigger={plainTrigger} ariaLabel="Row actions" open />
  </div>
{/snippet}

<Story name="Flips Above Near Bottom" template={flipUp} />

<!-- Long list: the panel scrolls rather than growing past the viewport. -->
{#snippet longList()}
  <div class="min-h-screen bg-crust p-6">
    <DropdownMenu items={MANY} trigger={plainTrigger} ariaLabel="Folders" open />
  </div>
{/snippet}

<Story name="Long List" template={longList} />
