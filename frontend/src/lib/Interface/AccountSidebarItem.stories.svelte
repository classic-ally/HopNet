<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import AccountSidebarItem from './AccountSidebarItem.svelte';

    const { Story } = defineMeta({
        title: 'Interface/AccountSidebarItem',
        component: AccountSidebarItem,
        argTypes: {
            selected: { control: 'boolean', description: 'Expanded account view' },
            onClick: { action: 'onClick' },
            onBack: { action: 'onBack' }
        },
        parameters: {
            docs: {
                description: {
                    component:
                        'The account row at the foot of the sidebar. Reads the signed-in user from ' +
                        'the store, so with no backend the display name falls back to its ' +
                        'placeholder — the point here is the row typography and the selected state.'
                }
            }
        }
    });
</script>

<!--
  Sidebar width and background, since a row this narrow reads quite differently
  stretched across a full viewport.
-->
{#snippet template(args)}
    <div class="w-64 bg-mantle p-2">
        <AccountSidebarItem {...args} />
    </div>
{/snippet}

<Story name="Collapsed" {template} args={{ selected: false }} />

<Story name="Selected" {template} args={{ selected: true }} />
