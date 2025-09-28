<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import Button from './Button.svelte';

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
      position: {
        control: 'radio',
        options: ['left', 'right'],
        description: 'Icon position relative to text'
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
    <div class="max-w-md mx-auto space-y-4">
      <h3 class="text-lg font-semibold text-primary mb-4">Button Component</h3>
      <Button {...args} />
    </div>
  </div>
{/snippet}

<Story
  name="Default (Icon Left)"
  {template}
  args={{
    icon: "i-carbon-add",
    text: "Add Item",
    position: "left",
    onClick: () => console.log('Button clicked!')
  }}
/>

<Story
  name="Icon Right"
  {template}
  args={{
    icon: "i-carbon-arrow-right",
    text: "Continue",
    position: "right",
    onClick: () => console.log('Continue clicked!')
  }}
/>

<Story
  name="Save Action"
  {template}
  args={{
    icon: "i-carbon-save",
    text: "Save Changes",
    position: "left",
    onClick: () => alert('Changes saved!')
  }}
/>

<Story
  name="Delete Action"
  {template}
  args={{
    icon: "i-carbon-trash-can",
    text: "Delete",
    position: "left",
    onClick: () => confirm('Are you sure you want to delete?')
  }}
/>

<Story
  name="Download Action"
  {template}
  args={{
    icon: "i-carbon-download",
    text: "Download",
    position: "left",
    onClick: () => console.log('Download started')
  }}
/>

<Story
  name="Settings"
  {template}
  args={{
    icon: "i-carbon-settings",
    text: "Settings",
    position: "left",
    onClick: () => console.log('Opening settings')
  }}
/>

<Story
  name="Upload"
  {template}
  args={{
    icon: "i-carbon-upload",
    text: "Upload Files",
    position: "left",
    onClick: () => console.log('File upload triggered')
  }}
/>

<Story
  name="External Link"
  {template}
  args={{
    icon: "i-carbon-launch",
    text: "Open External",
    position: "right",
    onClick: () => window.open('https://example.com', '_blank')
  }}
/>

<Story
  name="Long Text"
  {template}
  args={{
    icon: "i-carbon-information",
    text: "This is a button with longer text content",
    position: "left",
    onClick: () => console.log('Long text button clicked')
  }}
/>

<Story
  name="Interactive Demo"
  {template}
  args={{
    icon: "i-carbon-play",
    text: "Click Me!",
    position: "left",
    onClick: () => alert('Interactive button clicked! Check the console for more examples.')
  }}
/>