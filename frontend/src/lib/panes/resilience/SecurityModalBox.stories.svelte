<script module>
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import SecurityModalBox from './SecurityModalBox.svelte';
  import { SECURITY_OVERVIEW, STORY_CONFIGS } from './securityConstants';

  const { Story } = defineMeta({
    title: 'Components/SecurityModalBox',
    component: SecurityModalBox,
    argTypes: {
      bgColor: { control: 'text' },
      headerBgColor: { control: 'text' },
      borderColor: { control: 'text' },
      textColor: { control: 'text' },
      headerIcon: { control: 'text' },
      title: { control: 'text' }
    }
  });
</script>

{#snippet template(args)}
  <div class="p-4 bg-base min-h-screen">
    <SecurityModalBox {...args} />
  </div>
{/snippet}

<Story
  name="Items Mode"
  {template}
  args={STORY_CONFIGS.ITEMS_MODE}
/>

<Story
  name="Paragraphs Mode"
  {template}
  args={SECURITY_OVERVIEW}
/>

<Story
  name="Warning Style"
  {template}
  args={STORY_CONFIGS.WARNING_STYLE}
/>

<Story
  name="Info Style"
  {template}
  args={STORY_CONFIGS.INFO_STYLE}
/>