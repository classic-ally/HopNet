<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import TextInput from './TextInput.svelte';
    import Button from '../Button.svelte';

    const { Story } = defineMeta({
        title: 'Primitives/TextInput',
        component: TextInput,
        argTypes: {
            value: {
                control: 'text',
                description: 'Current value of the input'
            },
            placeholder: {
                control: 'text',
                description: 'Placeholder text'
            },
            disabled: {
                control: 'boolean',
                description: 'Whether the input is disabled'
            },
            type: {
                control: 'select',
                options: ['text', 'password', 'email', 'number'],
                description: 'Input type'
            },
            mode: {
                control: 'radio',
                options: ['desktop', 'mobile'],
                description: 'Device mode for sizing'
            }
        }
    });
</script>

<script lang="ts">
    function handleButtonClick() {
        console.log('Button clicked for comparison');
    }
</script>

<!-- Basic TextInput Story -->
<Story
    name="TextInput with Button Comparison"
    args={{
        value: '',
        placeholder: 'Enter text here...',
        disabled: false,
        type: 'text',
        mode: 'desktop'
    }}
>
    {#snippet template(args)}
        <div class="space-y-4 max-w-md mx-auto p-4">
            <div class="space-y-2">
                <h3 class="text-lg font-semibold text-primary">Visual Comparison</h3>
                <p class="text-sm text-muted">TextInput and Button should have matching visual weight and styling</p>
            </div>

            <div class="space-y-3">
                <div>
                    <label class="block text-sm font-medium text-subtitle mb-1">TextInput Component</label>
                    <TextInput {...args} />
                </div>

                <div>
                    <label class="block text-sm font-medium text-subtitle mb-1">Button Component (for comparison)</label>
                    <Button
                        icon="i-carbon-checkmark"
                        text="Submit"
                        onClick={handleButtonClick}
                    />
                </div>
            </div>

            <div class="flex items-center gap-2">
                <TextInput placeholder="Side by side..." />
                <Button
                    icon="i-carbon-send"
                    text="Send"
                    onClick={handleButtonClick}
                />
            </div>

            <div class="text-xs text-muted">
                <p>• Check that borders, corner radius, and colors match</p>
                <p>• Focus the input to see interactive states</p>
                <p>• Toggle controls to test different states</p>
            </div>
        </div>
    {/snippet}
</Story>