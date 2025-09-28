<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import Toolbar from './Toolbar.svelte';
  import type { ToolbarItem, ToolbarTabs } from './Toolbar.svelte';

  const { Story } = defineMeta({
    title: 'Primitives/Toolbar',
    component: Toolbar,
    argTypes: {
      mode: {
        control: 'radio',
        options: ['desktop', 'mobile'],
        description: 'Device mode for touch vs mouse targeting'
      }
    }
  });

  // Sample toolbar items with different compact stages
  const leftActions: ToolbarItem[] = [
    {
      type: 'action',
      icon: 'i-carbon-menu',
      text: 'Menu',
      onClick: () => console.log('Menu clicked'),
      compactStage: 2,
      tooltip: 'Open navigation menu'
    },
    {
      type: 'action',
      icon: 'i-carbon-list',
      text: 'View Mode',
      onClick: () => console.log('View mode clicked'),
      compactStage: 3 // More willing to compact
    }
  ];

  const centerActions: ToolbarItem[] = [
    {
      type: 'action',
      icon: 'i-carbon-upload',
      text: 'Upload Files',
      onClick: () => console.log('Upload clicked'),
      compactStage: 0, // Never compact
      tooltip: 'Upload files to server'
    },
    {
      type: 'action',
      icon: 'i-carbon-folder-add',
      text: 'New Folder',
      onClick: () => console.log('New folder clicked'),
      compactStage: 1, // Last to compact
      tooltip: 'Create a new folder in the current directory'
    }
  ];

  const rightActions: ToolbarItem[] = [
    {
      type: 'action',
      icon: 'i-carbon-download',
      text: 'Download',
      onClick: () => console.log('Download clicked'),
      compactStage: 0 // Essential - never compact
    },
    {
      type: 'action',
      icon: 'i-carbon-share',
      text: 'Share',
      onClick: () => console.log('Share clicked'),
      compactStage: 2
    },
    {
      type: 'action',
      icon: 'i-carbon-settings',
      text: 'Settings',
      onClick: () => console.log('Settings clicked'),
      compactStage: 3
    }
  ];

  // Toolbar with tabs in center
  const centerTabs: ToolbarTabs = {
    type: 'tabs',
    tabs: [
      { key: 'files', label: 'Files', icon: 'i-carbon-folder', color: 'mauve' },
      { key: 'recent', label: 'Recent', icon: 'i-carbon-time', color: 'mauve' },
      { key: 'shared', label: 'Shared', icon: 'i-carbon-share', color: 'mauve' }
    ],
    activeTab: 'files',
    onTabChange: (tab) => console.log('Tab changed to:', tab),
    compactStage: 1
  };
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6">
    <div class="space-y-6">
      <h3 class="text-lg font-semibold text-primary">Toolbar Component</h3>

      <div class="space-y-4">
        <h4 class="text-md font-medium text-primary">Responsive Test (Resize window to see compacting)</h4>
        <Toolbar
          mode={args.mode}
          leftElements={leftActions}
          centerElements={centerActions}
          rightElements={rightActions}
        />
        <p class="text-subtitle text-sm">
          Compact stages: View Mode(3) → Share(2), Settings(2) → Menu(2) → New Folder(1) → Upload Files(0), Download(0) never compact
        </p>
      </div>

      <div class="space-y-4">
        <h4 class="text-md font-medium text-primary">Toolbar with Center Tabs</h4>
        <Toolbar
          mode={args.mode}
          leftElements={leftActions.slice(0, 1)}
          centerElements={[centerTabs]}
          rightElements={rightActions.slice(0, 2)}
        />
      </div>

      <div class="space-y-4">
        <h4 class="text-md font-medium text-primary">Mobile Mode</h4>
        <Toolbar
          mode="mobile"
          leftElements={leftActions}
          centerElements={centerActions}
          rightElements={rightActions}
        />
        <p class="text-subtitle text-sm">All items use mobile variant regardless of space</p>
      </div>

      <div class="space-y-4">
        <h4 class="text-md font-medium text-primary">Minimal Toolbar</h4>
        <Toolbar
          mode={args.mode}
          leftElements={[leftActions[0]]}
          centerElements={[]}
          rightElements={[rightActions[0]]}
        />
      </div>
    </div>
  </div>
{/snippet}

{#snippet constrainedTest()}
  <div class="min-h-screen bg-base p-6">
    <div class="space-y-6">
      <h3 class="text-lg font-semibold text-primary">Constrained Width Test</h3>

      <div class="space-y-4">
        <h4 class="text-md font-medium text-primary">Very Narrow Container (300px)</h4>
        <div class="w-[300px] border border-overlay1 rounded">
          <Toolbar
            leftElements={leftActions}
            centerElements={centerActions}
            rightElements={rightActions}
          />
        </div>
      </div>

      <div class="space-y-4">
        <h4 class="text-md font-medium text-primary">Medium Container (500px)</h4>
        <div class="w-[500px] border border-overlay1 rounded">
          <Toolbar
            leftElements={leftActions}
            centerElements={centerActions}
            rightElements={rightActions}
          />
        </div>
      </div>

      <div class="space-y-4">
        <h4 class="text-md font-medium text-primary">Wide Container (800px)</h4>
        <div class="w-[800px] border border-overlay1 rounded">
          <Toolbar
            leftElements={leftActions}
            centerElements={centerActions}
            rightElements={rightActions}
          />
        </div>
      </div>
    </div>
  </div>
{/snippet}

<Story
  name="Default"
  {template}
  args={{
    mode: 'desktop'
  }}
/>

<Story
  name="Mobile Mode"
  {template}
  args={{
    mode: 'mobile'
  }}
/>

<Story
  name="Constrained Width Test"
  template={constrainedTest}
  args={{}}
/>

{#snippet debugTemplate(args)}
  <div class="min-h-screen bg-base p-6">
    <div class="space-y-6">
      <h3 class="text-lg font-semibold text-primary">Debug: Responsive Compacting (Check Console)</h3>

      <div class="space-y-4">
        <p class="text-subtitle text-sm">
          Resize window or browser dev tools to trigger compacting. Watch console for debug output.
        </p>
        <p class="text-subtitle text-sm">
          Compact stages: View Mode(3) → Share(2), Settings(2) → Menu(2) → New Folder(1) → Upload Files(0), Download(0) never compact
        </p>

        <Toolbar
          mode={args.mode}
          leftElements={leftActions}
          centerElements={centerActions}
          rightElements={rightActions}
        />
      </div>
    </div>
  </div>
{/snippet}

<Story
  name="Debug Responsive (Console Only)"
  template={debugTemplate}
  args={{
    mode: 'desktop'
  }}
/>