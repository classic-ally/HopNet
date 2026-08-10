<script lang="ts">
    import { liveFilePreviewApi, type FilePreviewApi } from '../api/filePreview'
    import Button from '../Button.svelte'
    import { onMount, untrack } from 'svelte'
    import Modal from '../primitives/Modal.svelte'
    import type { FileItem } from '../types'
    import { InodeType } from '../types'
    import { getFileName, getFileExtension, getFileIcon } from '../utils/formatters'

    // Dynamically load MonacoEditor when preview opens
    let MonacoEditor: any = $state(null)

    // Supported preview file types - extensible for future additions
    const SUPPORTED_PREVIEW_TYPES = {
        pdf: {
            extensions: ['pdf'],
            mimeType: 'application/pdf',
            component: 'embed',
            maxSize: null, // No size limit for PDFs
        },
        code: {
            extensions: ['js', 'jsx', 'ts', 'tsx', 'py', 'rs', 'go', 'java', 'cpp', 'c', 'h', 'cs', 'php', 'rb', 'sql', 'sh', 'bash', 'ps1', 'dockerfile'],
            mimeType: 'text/plain',
            component: 'monaco',
            maxSize: 1024 * 1024, // 1MB limit for code files
        },
        text: {
            extensions: ['txt', 'md', 'log', 'yml', 'yaml', 'xml', 'csv', 'json', 'toml', 'ini', 'conf'],
            mimeType: 'text/plain',
            component: 'pre',
            maxSize: 1024 * 1024, // 1MB limit for text files
        },
        image: {
            extensions: ['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp'],
            mimeType: 'image/*',
            component: 'img',
            maxSize: null,
        },
    } as const

    type PreviewType = keyof typeof SUPPORTED_PREVIEW_TYPES

    function getPreviewType(filename: string): PreviewType | null {
        const extension = getFileExtension(filename).toLowerCase()

        for (const [type, config] of Object.entries(SUPPORTED_PREVIEW_TYPES)) {
            if ((config.extensions as readonly string[]).includes(extension)) {
                return type as PreviewType
            }
        }

        return null
    }

    function getMonacoLanguage(filename: string): string {
        const extension = getFileExtension(filename).toLowerCase()

        // Map file extensions to Monaco language IDs
        const languageMap: Record<string, string> = {
            'js': 'javascript',
            'jsx': 'javascript',
            'ts': 'typescript',
            'tsx': 'typescript',
            'py': 'python',
            'rs': 'rust',
            'go': 'go',
            'java': 'java',
            'cpp': 'cpp',
            'c': 'c',
            'h': 'c',
            'cs': 'csharp',
            'php': 'php',
            'rb': 'ruby',
            'sql': 'sql',
            'sh': 'shell',
            'bash': 'shell',
            'ps1': 'powershell',
            'dockerfile': 'dockerfile',
            'css': 'css',
            'scss': 'scss',
            'sass': 'sass',
            'less': 'less',
            'html': 'html',
            'xml': 'xml',
            'json': 'json',
            'yaml': 'yaml',
            'yml': 'yaml',
            'md': 'markdown',
            'toml': 'toml',
        }

        return languageMap[extension] || 'plaintext'
    }

    const {
        file,
        fileList = [],
        currentIndex = 0,
        onClose,
        onNavigate = null,
        api = liveFilePreviewApi
    }: {
        file: FileItem,
        fileList?: FileItem[],
        currentIndex?: number,
        onClose: () => void,
        onNavigate?: ((newIndex: number) => void) | null,
        /** Injectable content fetch; see api/filePreview.ts. */
        api?: FilePreviewApi
    } = $props()

    let loading = $state(true)
    let error = $state('')
    let previewUrl: string | null = $state(null)
    let textContent: string | null = $state(null)
    let fileTooLarge = $state(false)

    // Validate that we have a valid file and index
    const validFile = $derived(file && currentIndex >= 0 && currentIndex < fileList.length)

    // Make filename and previewType reactive to file changes
    const filename = $derived(getFileName(file.path))
    const previewType = $derived(getPreviewType(filename))

    async function fetchFilePreview() {
        loading = true
        error = ''

        try {
            // Nothing to fetch for a type we cannot render.
            if (!previewType) return

            const config = SUPPORTED_PREVIEW_TYPES[previewType]

            // Oversized text and code are refused before the request, not after.
            if (config.maxSize && file.file_size) {
                if (parseInt(file.file_size) > config.maxSize) {
                    fileTooLarge = true
                    return
                }
            }
            fileTooLarge = false

            if (previewType === 'text' || previewType === 'code') {
                const result = await api.fetchText(file.path)
                if (result.ok) textContent = result.text
                else error = result.detail
            } else {
                const result = await api.fetchBlob(file.path, config.mimeType)
                if (result.ok) previewUrl = result.url
                else error = result.detail
            }
        } finally {
            loading = false
        }
    }

    async function downloadFile() {
        const result = await api.download(file.path, filename)
        if (!result.ok) {
            error = result.detail
            return
        }

        // The save itself stays here: the seam hands back bytes and a name, and
        // reaching into the DOM is the component's job, not the API layer's.
        const a = document.createElement('a')
        a.href = result.url
        a.download = result.filename
        document.body.appendChild(a)
        a.click()
        URL.revokeObjectURL(result.url)
        document.body.removeChild(a)

        onClose()
    }

    /**
     * Arrows only. Escape belongs to Modal, which closes on it by default —
     * handling it here as well would call onClose twice.
     */
    function handleKeydown(event: KeyboardEvent) {
        if (event.key === 'ArrowLeft' && !loading) {
            previousFile()
        } else if (event.key === 'ArrowRight' && !loading) {
            nextFile()
        }
    }

    function previousFile() {
        if (currentIndex > 0 && onNavigate && fileList.length > 0) {
            onNavigate(currentIndex - 1)
        }
    }

    function nextFile() {
        if (currentIndex < fileList.length - 1 && onNavigate && fileList.length > 0 && currentIndex < fileList.length) {
            onNavigate(currentIndex + 1)
        }
    }

    function cleanupPreviousPreview() {
        if (previewUrl) {
            URL.revokeObjectURL(previewUrl)
            previewUrl = null
        }
        textContent = null
        fileTooLarge = false
    }

    // Refetch preview when file changes
    // Re-fetch only when the file prop changes — untrack the side effects
    // to prevent $effect from re-triggering when fetchFilePreview writes to $state vars
    $effect(() => {
        file;  // track only the file prop
        untrack(() => {
            cleanupPreviousPreview()
            fetchFilePreview()
        })
    })

    onMount(() => {
        // Preload MonacoEditor component
        import('../components/MonacoEditor.svelte').then(monacoModule => {
            MonacoEditor = monacoModule.default
        })

        document.addEventListener('keydown', handleKeydown)

        return () => {
            document.removeEventListener('keydown', handleKeydown)
            if (previewUrl) {
                URL.revokeObjectURL(previewUrl)
            }
        }
    })
</script>

<!--
  Chrome comes from Modal: backdrop, panel, header, footer rule, Escape and
  backdrop dismissal. This file previously reimplemented all of it, down to a
  hand-drawn SVG cross where every other dialog uses the icon button.

  `loading` and `error` are deliberately not handed to Modal — the preview shows
  its own in-content states, and Modal's banners would duplicate them.
-->
<Modal
    title={filename}
    titleIcon={getFileIcon(file.inode_type === InodeType.Folder ? 'Folder' : 'File', filename, 'detail')}
    size="2xl"
    height="tall"
    contentPadding={false}
    {onClose}
>
    {#snippet headerActions()}
        <Button
            icon="i-carbon-download"
            text="Download"
            onClick={downloadFile}
            disabled={loading}
        />
    {/snippet}

    {#snippet content()}
            {#if loading}
                <div class="flex items-center justify-center h-64">
                    <div class="text-muted">Loading preview...</div>
                </div>
            {:else if error}
                <div class="p-4">
                    <div class="text-red p-4 border border-red rounded bg-red/10">
                        {error}
                    </div>
                </div>
            {:else if previewType === 'pdf' && previewUrl}
                <!-- PDF Preview -->
                <div class="h-full">
                    <embed
                        src={previewUrl}
                        type="application/pdf"
                        class="w-full h-full"
                        title="PDF Preview"
                    />
                </div>
            {:else if previewType === 'image' && previewUrl}
                <!-- Image Preview -->
                <div class="h-full flex items-center justify-center p-4 overflow-auto">
                    <img src={previewUrl} alt={filename} class="max-w-full max-h-full object-contain" />
                </div>
            {:else if (previewType === 'text' || previewType === 'code') && fileTooLarge}
                <!-- File Too Large Warning -->
                <div class="flex items-center justify-center h-full">
                    <div class="text-center">
                        <div class="text-6xl text-muted mb-4">📄</div>
                        <div class="text-muted">File too large for preview</div>
                        <div class="text-sm text-muted mt-2">Files larger than 1MB are not previewed for performance</div>
                        <div class="text-xs text-muted mt-1">File size: {(parseInt(file.file_size) / 1024 / 1024).toFixed(1)}MB</div>
                    </div>
                </div>
            {:else if previewType === 'code' && textContent !== null}
                <!-- Code Preview with Monaco Editor -->
                <div class="h-full">
                    {#if MonacoEditor}
                        <MonacoEditor
                            value={textContent}
                            language={getMonacoLanguage(filename)}
                            theme="catppuccin-mocha"
                            readOnly={true}
                        />
                    {:else}
                        <!-- Loading Monaco Editor -->
                        <div class="flex items-center justify-center h-64">
                            <div class="text-muted">Loading editor...</div>
                        </div>
                    {/if}
                </div>
            {:else if previewType === 'text' && textContent !== null}
                <!-- Text Preview -->
                <div class="h-full overflow-auto">
                    <pre class="whitespace-pre-wrap text-sm font-mono p-4 leading-relaxed">{textContent}</pre>
                </div>
            {:else if previewType}
                <!-- Future preview types can be added here -->
                <div class="flex items-center justify-center h-full">
                    <div class="text-center">
                        <div class="text-6xl text-muted mb-4">⚠️</div>
                        <div class="text-muted">Preview renderer not implemented for {previewType}</div>
                    </div>
                </div>
            {:else}
                <!-- Unsupported Preview -->
                <div class="flex items-center justify-center h-full">
                    <div class="text-center">
                        <div class="text-6xl text-muted mb-4">📄</div>
                        <div class="text-muted">Preview not supported for this file type</div>
                        <div class="text-sm text-muted mt-2">File extension: .{getFileExtension(filename)}</div>
                    </div>
                </div>
            {/if}
    {/snippet}

    {#snippet footer()}
        {#if fileList.length > 1 && onNavigate}
            <div class="flex items-center justify-between">
                <Button
                    icon="i-carbon-chevron-left"
                    text="Previous"
                    onClick={previousFile}
                    disabled={currentIndex === 0 || fileList.length === 0 || loading}
                />

                <div class="text-center">
                    <div class="text-sm text-muted">
                        {validFile ? currentIndex + 1 : '?'} of {fileList.length}
                    </div>
                    <div class="text-xs text-muted mt-1">
                        Use ← → arrow keys to navigate
                    </div>
                </div>

                <Button
                    icon="i-carbon-chevron-right"
                    text="Next"
                    onClick={nextFile}
                    position="right"
                    disabled={currentIndex >= fileList.length - 1 || fileList.length === 0 || loading}
                />
            </div>
        {/if}
    {/snippet}
</Modal>