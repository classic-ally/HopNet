<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import Checkbox from './Checkbox.svelte';

  const { Story } = defineMeta({
    title: 'Primitives/Checkbox',
    component: Checkbox,
    argTypes: {
      checked: { control: 'boolean' },
      indeterminate: { control: 'boolean' },
      disabled: { control: 'boolean' },
      invalid: { control: 'boolean' },
      label: { control: 'text' },
      ariaLabel: { control: 'text' },
      onCheckedChange: { action: 'onCheckedChange' }
    },
    parameters: {
      docs: {
        description: {
          component:
            'A real <input type="checkbox">, visually hidden with the square drawn beside it, so ' +
            'space-to-toggle, form participation and the announced state come from the platform. ' +
            'Supports both bind:checked and checked + onCheckedChange, since the codebase uses each.'
        }
      }
    }
  });
</script>

{#snippet template(args)}
  <div class="p-6 flex flex-col gap-4">
    <Checkbox {...args} />
  </div>
{/snippet}

<Story name="Unchecked" {template} args={{ label: 'Remember me for 24 hours' }} />

<Story name="Checked" {template} args={{ label: 'Remember me for 24 hours', checked: true }} />

<Story
  name="Indeterminate"
  {template}
  args={{ label: 'Select all rows', indeterminate: true }}
/>

<Story name="Disabled" {template} args={{ label: 'Not available yet', disabled: true }} />

<Story
  name="Disabled Checked"
  {template}
  args={{ label: 'Locked on', checked: true, disabled: true }}
/>

<Story name="Invalid" {template} args={{ label: 'You must accept this', invalid: true }} />

<!-- No visible label: the naming has to come from ariaLabel, as in a table cell. -->
<Story name="Bare (table cell)" {template} args={{ ariaLabel: 'Select this row' }} />

<!--
  Every state at once, for eyeballing alignment and the tick's optical centring.
  Tab through it to check the focus ring: it is driven off the hidden input's
  :focus-visible, so it should appear on keyboard focus and not on click.
-->
{#snippet gallery()}
  <div class="p-6 flex flex-col gap-3">
    <Checkbox label="Unchecked" />
    <Checkbox label="Checked" checked />
    <Checkbox label="Indeterminate" indeterminate />
    <Checkbox label="Invalid" invalid />
    <Checkbox label="Invalid and checked" invalid checked />
    <Checkbox label="Disabled" disabled />
    <Checkbox label="Disabled and checked" checked disabled />
    <Checkbox ariaLabel="Bare, no visible label" />
  </div>
{/snippet}

<Story name="All States" template={gallery} args={{}} />
