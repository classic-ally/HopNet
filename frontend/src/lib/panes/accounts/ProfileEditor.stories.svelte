<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import ProfileEditor from './ProfileEditor.svelte';
    import { currentUserStore } from '../../stores';

    /**
     * The component is gated on `{#if $currentUserStore}`, so without a signed-in
     * user it renders nothing at all. Seed the store directly rather than mock the
     * fetch: it is the only input the card reads, and setting it keeps the story
     * honest about what the card needs to exist.
     */
    currentUserStore.set({
        user_id: 1,
        username: 'allison',
        first_name: 'Allison',
        last_name: 'Bentley',
        onboarding_flags: 0
    });

    const { Story } = defineMeta({
        title: 'Panes/Accounts/ProfileEditor',
        component: ProfileEditor,
        argTypes: {
            onSaved: {
                action: 'onSaved',
                description: 'Fired after a successful name save'
            }
        },
        parameters: {
            docs: {
                description: {
                    component:
                        'The "My Profile" card: avatar, first and last name, and the save action. ' +
                        'Writes through the accounts API, so saving fails with no backend — the ' +
                        'point here is the card heading and layout.'
                }
            }
        }
    });
</script>

<!-- Sits inside AccountsPane in the app, which supplies the page padding. -->
{#snippet template(args)}
    <div class="p-5 max-w-2xl">
        <ProfileEditor {...args} />
    </div>
{/snippet}

<Story name="Default" {template} args={{}} />
