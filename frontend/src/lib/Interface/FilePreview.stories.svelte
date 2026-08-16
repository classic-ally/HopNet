<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import FilePreview from './FilePreview.svelte';
  import { InodeType, type FileItem } from '../types';
  import { mockFilePreviewApi, SAMPLE_CODE, SAMPLE_TEXT, SAMPLE_IMAGE_URL } from '../api/filePreview.mock';

  function fileAt(path: string, size = '20480'): FileItem {
    return {
      id: path,
      path,
      inode_type: InodeType.File,
      file_size: size,
      creation_date: '2026-08-01T09:00:00Z',
      modification_date: '2026-08-09T17:20:00Z'
    };
  }

  // The preview type is derived from the extension, so the filename is what
  // selects each branch.
  const CODE = fileAt('/src/upgrade/nix_provider.rs', '2400');
  const TEXT = fileAt('/notes/RELEASES.md', '1100');
  const IMAGE = fileAt('/photos/holiday.png', '482000');
  const PDF = fileAt('/reports/quarterly.pdf', '1840000');
  const HUGE = fileAt('/logs/node.log', String(4 * 1024 * 1024));
  const ODD = fileAt('/archive/backup.xyz', '9100');

  const LIST = [CODE, TEXT, IMAGE];

  const { Story } = defineMeta({
    title: 'Interface/FilePreview',
    component: FilePreview,
    argTypes: {
      currentIndex: { control: 'number' },
      onClose: { action: 'onClose' },
      onNavigate: { action: 'onNavigate' }
    },
    parameters: {
      docs: {
        description: {
          component:
            'The file preview dialog. Chrome comes from Modal — panel, header with the ' +
            'file-type icon and Download action, and the footer navigation. Content is ' +
            'fetched through an injectable seam (api/filePreview.ts), so every render branch ' +
            'is reachable here without a backend.'
        }
      }
    }
  });
</script>

<!-- Modal positions itself; the wrapper is only the page behind it. -->
{#snippet one(args)}
  <div class="min-h-screen bg-crust">
    <FilePreview
      file={args.file}
      api={mockFilePreviewApi(args.mock)}
      onClose={() => console.log('close')}
    />
  </div>
{/snippet}

<!-- Code goes through Monaco, which is dynamically imported — expect a beat. -->
<Story name="Code (Monaco)" template={one} args={{ file: CODE, mock: { text: SAMPLE_CODE } }} />

<Story name="Text" template={one} args={{ file: TEXT, mock: { text: SAMPLE_TEXT } }} />

<Story name="Image" template={one} args={{ file: IMAGE, mock: { url: SAMPLE_IMAGE_URL } }} />

<!--
  An <embed> of a PDF data URI is at the browser's discretion — Chrome renders
  it, some engines refuse. The story is here for the chrome around it.
-->
<Story
  name="PDF"
  template={one}
  args={{
    file: PDF,
    mock: {
      url:
        'data:application/pdf;base64,JVBERi0xLjQKMSAwIG9iago8PC9UeXBlL0NhdGFsb2cvUGFnZXMgMiAwIFI+' +
        'PmVuZG9iagoyIDAgb2JqCjw8L1R5cGUvUGFnZXMvS2lkc1szIDAgUl0vQ291bnQgMT4+ZW5kb2JqCjMgMCBvYmoK' +
        'PDwvVHlwZS9QYWdlL1BhcmVudCAyIDAgUi9NZWRpYUJveFswIDAgMjAwIDIwMF0+PmVuZG9iagp0cmFpbGVyCjw8' +
        'L1Jvb3QgMSAwIFI+Pg=='
    }
  }}
/>

<!-- Over the 1MB text ceiling: refused before the request is made. -->
<Story name="Too Large" template={one} args={{ file: HUGE, mock: {} }} />

<!-- No preview branch for this extension; nothing is fetched. -->
<Story name="Unsupported Type" template={one} args={{ file: ODD, mock: {} }} />

<Story
  name="Fetch Failed"
  template={one}
  args={{ file: TEXT, mock: { failWith: 'Failed to load preview: 503' } }}
/>

<Story
  name="No Auth Token"
  template={one}
  args={{ file: TEXT, mock: { failWith: 'No authentication token found' } }}
/>

<!-- Long latency parks it in the loading state. -->
<Story
  name="Loading"
  template={one}
  args={{ file: TEXT, mock: { text: SAMPLE_TEXT, latencyMs: 600_000 } }}
/>

<!--
  With a list and a navigate handler the footer appears, carrying prev / the
  "n of m" counter / next. Arrow keys drive it too; Escape belongs to Modal.
-->
{#snippet navigable(args)}
  <div class="min-h-screen bg-crust">
    <FilePreview
      file={LIST[args.currentIndex]}
      fileList={LIST}
      currentIndex={args.currentIndex}
      api={mockFilePreviewApi({ text: SAMPLE_CODE, url: SAMPLE_IMAGE_URL })}
      onClose={() => console.log('close')}
      onNavigate={(i) => console.log('navigate', i)}
    />
  </div>
{/snippet}

<Story name="Navigation — first" template={navigable} args={{ currentIndex: 0 }} />

<Story name="Navigation — middle" template={navigable} args={{ currentIndex: 1 }} />

<Story name="Navigation — last" template={navigable} args={{ currentIndex: 2 }} />
