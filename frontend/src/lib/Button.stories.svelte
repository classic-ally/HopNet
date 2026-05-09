<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import Button from './Button.svelte';
  import Tabs from './primitives/Tabs.svelte';

  const { Story } = defineMeta({
    title: 'Primitives/Button',
    component: Button,
    argTypes: {
      icon: {
        control: 'text',
        description: 'CSS class for the icon (e.g., "i-carbon-add")'
      },
      text: {
        control: 'text',
        description: 'Button text content'
      },
      tooltip: {
        control: 'text',
        description: 'Optional tooltip override (defaults to text)'
      },
      variant: {
        control: 'radio',
        options: ['desktop', 'compact', 'mobile', 'card'],
        description: 'Display variant for different contexts'
      },
      position: {
        control: 'radio',
        options: ['left', 'right'],
        description: 'Icon position relative to text (desktop only)'
      },
      disabled: {
        control: 'boolean',
        description: 'Disabled state'
      },
      onClick: {
        control: false,
        description: 'Click handler function'
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="min-h-screen bg-base p-6">
    <div class="max-w-2xl mx-auto space-y-6">
      <h3 class="text-lg font-semibold text-primary mb-4">Button Component</h3>
      <Button {...args} />
    </div>
  </div>
{/snippet}

{#snippet variantComparison()}
  <div class="min-h-screen bg-base p-6">
    <div class="max-w-4xl mx-auto space-y-6">
      <h3 class="text-lg font-semibold text-primary mb-4">Button Variants Comparison</h3>

      <div class="space-y-4">
        <h4 class="text-md font-medium text-primary">All Variants (Same Button)</h4>
        <div class="flex items-center gap-4">
          <Button icon="i-carbon-add" text="Add Item" variant="desktop" onClick={() => console.log('Desktop')} />
          <Button icon="i-carbon-add" text="Add Item" variant="compact" onClick={() => console.log('Compact')} />
          <Button icon="i-carbon-add" text="Add Item" variant="mobile" onClick={() => console.log('Mobile')} />
          <span class="text-subtitle text-sm">← Desktop, Compact, Mobile</span>
        </div>
      </div>

      <div class="space-y-4">
        <h4 class="text-md font-medium text-primary">Mixed Desktop Toolbar</h4>
        <div class="flex items-center gap-1 p-2 bg-surface0 rounded-lg">
          <Button icon="i-carbon-menu" text="Menu" variant="compact" onClick={() => {}} />
          <Button icon="i-carbon-upload" text="Upload Files" variant="desktop" onClick={() => {}} />
          <Button icon="i-carbon-folder-add" text="New Folder" variant="compact" tooltip="Create a new folder in the current directory" onClick={() => {}} />
          <div class="flex-1 flex justify-center">
            <Tabs
              tabs={[
                { key: 'files', label: 'Files', color: 'mauve' },
                { key: 'recent', label: 'Recent', color: 'mauve' },
                { key: 'shared', label: 'Shared', color: 'mauve' }
              ]}
              activeTab="files"
              centered={false}
            />
          </div>
          <Button icon="i-carbon-download" text="Download" variant="desktop" onClick={() => {}} />
          <Button icon="i-carbon-share" text="Share" variant="compact" onClick={() => {}} />
          <Button icon="i-carbon-settings" text="Settings" variant="compact" onClick={() => {}} />
        </div>
        <p class="text-subtitle text-sm">↑ Mix of desktop (important actions) and compact (secondary actions) variants with tabs</p>
      </div>

      <div class="space-y-4">
        <h4 class="text-md font-medium text-primary">Mobile Toolbar</h4>
        <div class="flex items-center gap-1 p-2 bg-surface0 rounded-lg">
          <Button icon="i-carbon-menu" text="Menu" variant="mobile" onClick={() => {}} />
          <Button icon="i-carbon-upload" text="Upload" variant="mobile" onClick={() => {}} />
          <Button icon="i-carbon-folder-add" text="New Folder" variant="mobile" onClick={() => {}} />
          <div class="flex-1"></div>
          <Button icon="i-carbon-download" text="Download" variant="mobile" onClick={() => {}} />
        </div>
      </div>
    </div>
  </div>
{/snippet}

<Story
  name="Default (Desktop)"
  {template}
  args={{
    icon: "i-carbon-add",
    text: "Add Item",
    variant: "desktop",
    position: "left",
    onClick: () => console.log('Button clicked!')
  }}
/>

<Story
  name="Compact Variant"
  {template}
  args={{
    icon: "i-carbon-upload",
    text: "Upload",
    variant: "compact",
    tooltip: "Upload files to server",
    onClick: () => console.log('Compact button clicked!')
  }}
/>

<Story
  name="Mobile Variant"
  {template}
  args={{
    icon: "i-carbon-settings",
    text: "Settings",
    variant: "mobile",
    onClick: () => console.log('Mobile button clicked!')
  }}
/>

<Story
  name="Variant Comparison"
  template={variantComparison}
  args={{}}
/>

<Story
  name="Icon Right (Desktop)"
  {template}
  args={{
    icon: "i-carbon-arrow-right",
    text: "Continue",
    variant: "desktop",
    position: "right",
    onClick: () => console.log('Continue clicked!')
  }}
/>

<Story
  name="Disabled States"
  {template}
  args={{
    icon: "i-carbon-save",
    text: "Save Changes",
    variant: "desktop",
    disabled: true,
    onClick: () => alert('This should not fire!')
  }}
/>

<Story
  name="Custom Tooltip"
  {template}
  args={{
    icon: "i-carbon-folder-add",
    text: "New Folder",
    variant: "compact",
    tooltip: "Create a new folder in the current directory",
    onClick: () => console.log('Creating folder...')
  }}
/>

<!-- Card variant — full-width two-row button used by checklists and
     menu-style entry points. -->
<Story
  name="Card — basic"
  {template}
  args={{
    icon: "i-carbon-cloud-upload",
    text: "Import existing data",
    subtitle: "Bring in a HopNet takeout archive from a previous installation",
    variant: "card",
    onClick: () => console.log('Card clicked!'),
  }}
/>

<Story
  name="Card — with trailing icon + cta"
  {template}
  args={{
    icon: "i-carbon-cloud-upload",
    text: "Import existing data",
    subtitle: "Bring in a HopNet takeout archive from a previous installation",
    variant: "card",
    trailing: "i-carbon-circle-dash",
    trailingClass: "text-muted",
    trailingText: "Start",
    onClick: () => console.log('Card clicked!'),
  }}
/>

<Story
  name="Card — done state"
  {template}
  args={{
    icon: "i-carbon-cloud-upload",
    text: "Import existing data",
    subtitle: "Bring in a HopNet takeout archive from a previous installation",
    variant: "card",
    trailing: "i-carbon-checkmark-filled",
    trailingClass: "text-green",
    trailingText: "Review",
    onClick: () => console.log('Card clicked!'),
  }}
/>

<Story
  name="Card — disabled"
  {template}
  args={{
    icon: "i-carbon-cloud-upload",
    text: "Import existing data",
    subtitle: "Bring in a HopNet takeout archive from a previous installation",
    variant: "card",
    disabled: true,
    onClick: () => alert('This should not fire!'),
  }}
/>