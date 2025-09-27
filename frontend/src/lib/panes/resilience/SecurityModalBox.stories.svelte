<script module>
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import SecurityModalBox from './SecurityModalBox.svelte';

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
  args={{
    title: "Protected Against",
    bgColor: "green/5",
    borderColor: "border-green/30",
    textColor: "green",
    headerIcon: "checkmark-filled",
    items: [
      {
        title: "Individual node failures",
        subtitles: ["Network continues operating when individual computers crash, lose power, or become unreachable"],
        icon: "warning"
      },
      {
        title: "Maintenance availability",
        subtitles: ["System remains online during planned maintenance, updates, or configuration changes"],
        icon: "tools"
      }
    ]
  }}
/>

<Story
  name="Paragraphs Mode"
  {template}
  args={{
    title: "Understanding HopNet's Security Design",
    bgColor: "surface1/50",
    headerBgColor: "surface1",
    borderColor: "border-surface1/30",
    textColor: "primary",
    headerIcon: "information",
    paragraphs: [
      "Distributed storage systems face fundamental trade-offs between maximum uptime and defending against coordination attacks. HopNet prioritizes system continuity by providing strong file encryption regardless of network size, while consensus protections scale with your infrastructure.",
      "HopNet's adaptive security approach means you get meaningful protection immediately, starting with per-file encryption and growing into full Byzantine fault tolerance as you expand your HopNet deployment."
    ]
  }}
/>

<Story
  name="Warning Style"
  {template}
  args={{
    title: "Not Protected Against",
    bgColor: "red/5",
    borderColor: "border-red/30",
    textColor: "red",
    headerIcon: "warning-filled",
    items: [
      {
        title: "Coordinated malicious behavior by majority",
        subtitles: ["Validator majority conspiracy (50%+1) can issue illegitimate operations"],
        icon: "user-multiple"
      },
      {
        title: "Selective service denial by validators",
        subtitles: ["Validator majority conspiracy can block legitimate transactions"],
        icon: "block-storage"
      }
    ]
  }}
/>

<Story
  name="Info Style"
  {template}
  args={{
    title: "Always Protected",
    bgColor: "blue/5",
    borderColor: "border-blue/30",
    textColor: "blue",
    headerIcon: "locked",
    items: [
      {
        title: "Files are encrypted individually",
        subtitles: [
          "Each file gets its own encryption key, so compromising one file doesn't affect others",
          "Only people you explicitly grant access can decrypt your files"
        ],
        icon: "document-security"
      }
    ]
  }}
/>