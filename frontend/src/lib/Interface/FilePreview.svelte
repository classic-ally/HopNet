<script lang="ts">
    import { tokenStore, API_BASE_URL } from '../stores'
    import Button from '../Button.svelte'
    import { onMount } from 'svelte'
    import type { FileItem } from '../types'
    import { InodeType } from '../types'
    import { getFileName, getFileExtension, getFileIcon } from '../utils/formatters'
    import MonacoEditor from '../components/MonacoEditor.svelte'

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
        }
        // Future support could include:
        // image: {
        //     extensions: ['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp'],
        //     mimeType: 'image/*',
        //     component: 'img',
        //     maxSize: null,
        // },
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

    export let file: FileItem
    export let fileList: FileItem[] = []
    export let currentIndex: number = 0
    export let onClose: () => void
    export let onNavigate: ((newIndex: number) => void) | null = null

    let loading = true
    let error = ''
    let previewUrl: string | null = null
    let textContent: string | null = null
    let fileTooLarge = false
    let suppressClose = false

    // Validate that we have a valid file and index
    $: validFile = file && currentIndex >= 0 && currentIndex < fileList.length

    // Make filename and previewType reactive to file changes
    $: filename = getFileName(file.path)
    $: previewType = getPreviewType(filename)

    async function fetchFilePreview() {
        try {
            loading = true
            error = ''

            const token = $tokenStore
            if (!token) {
                error = 'No authentication token found'
                return
            }

            // Only fetch file content if we can preview it
            if (previewType) {
                const config = SUPPORTED_PREVIEW_TYPES[previewType]

                // Check file size limit for text files
                if (config.maxSize && file.file_size) {
                    const fileSizeBytes = parseInt(file.file_size)
                    if (fileSizeBytes > config.maxSize) {
                        fileTooLarge = true
                        loading = false
                        return
                    }
                }

                fileTooLarge = false

                // Extract the path part after the first slash for the API call
                let apiPath = file.path
                if (apiPath.startsWith('/')) {
                    apiPath = apiPath.substring(1)
                }

                // Fetch the file content for preview
                const previewResponse = await fetch(`${API_BASE_URL}/files/${apiPath}`, {
                    method: 'GET',
                    headers: {
                        'Authorization': `Bearer ${token}`,
                    },
                })

                if (previewResponse.ok) {
                    if (previewType === 'text' || previewType === 'code') {
                        // Handle text and code files
                        textContent = await previewResponse.text()
                    } else {
                        // Handle binary files (PDFs, etc.)
                        const blob = await previewResponse.blob()
                        const typedBlob = new Blob([blob], { type: config.mimeType })
                        previewUrl = URL.createObjectURL(typedBlob)
                    }
                } else {
                    console.warn('Failed to fetch preview:', previewResponse.status)
                    error = `Failed to load preview: ${previewResponse.status}`
                }
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`
            console.error('Error fetching file preview:', err)
        } finally {
            loading = false
        }
    }

    async function downloadFile() {
        try {
            const token = $tokenStore
            if (!token) {
                error = 'No authentication token found'
                return
            }

            // Extract the path part after the first slash for the API call
            let apiPath = file.path
            if (apiPath.startsWith('/')) {
                apiPath = apiPath.substring(1)
            }

            const response = await fetch(`${API_BASE_URL}/files/${apiPath}`, {
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${token}`,
                },
            })

            if (response.ok) {
                // Get the filename from the response headers or extract from path
                const contentDisposition = response.headers.get('Content-Disposition')
                let downloadFilename = filename

                // Try to extract filename from Content-Disposition header if present
                if (contentDisposition) {
                    const match = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/)
                    if (match && match[1]) {
                        downloadFilename = match[1].replace(/['"]/g, '')
                    }
                }

                // Create blob and trigger download
                const blob = await response.blob()
                const url = window.URL.createObjectURL(blob)
                const a = document.createElement('a')
                a.href = url
                a.download = downloadFilename
                document.body.appendChild(a)
                a.click()
                window.URL.revokeObjectURL(url)
                document.body.removeChild(a)

                // Close the preview after download
                onClose()
            } else {
                error = `Failed to download file: ${response.status} ${response.statusText}`
                console.error('Failed to download file:', response.status)
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`
            console.error('Error downloading file:', err)
        }
    }

    function handleBackdropMouseDown(event: MouseEvent) {
        if (event.target === event.currentTarget) {
            onClose()
        }
    }

    function handleKeydown(event: KeyboardEvent) {
        if (event.key === 'Escape') {
            onClose()
        } else if (event.key === 'ArrowLeft' && !loading) {
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

    // Reactive statement to refetch preview when file changes
    $: if (file) {
        cleanupPreviousPreview()
        fetchFilePreview()
    }

    onMount(() => {
        document.addEventListener('keydown', handleKeydown)

        return () => {
            document.removeEventListener('keydown', handleKeydown)
            if (previewUrl) {
                URL.revokeObjectURL(previewUrl)
            }
        }
    })
</script>

<!-- Backdrop -->
<div
    class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4"
    onmousedown={handleBackdropMouseDown}
>
    <!-- Modal -->
    <div class="bg-surface0 border border-overlay1 rounded-lg shadow-xl w-full max-w-4xl h-[90vh] overflow-hidden flex flex-col">
        <!-- Header -->
        <div class="flex items-center justify-between p-4 border-b border-overlay0">
            <div class="flex items-center gap-2 flex-1 mr-4 min-w-0">
                <div class="{getFileIcon(file.inode_type === InodeType.Folder ? 'Folder' : 'File', filename, 'detail')} w-5 h-5 text-primary flex-shrink-0"></div>
                <h3 class="text-lg font-semibold text-white truncate">{filename}</h3>
            </div>
            <div class="flex gap-2 items-center">
                <Button
                    icon="i-carbon-download"
                    text="Download"
                    onClick={downloadFile}
                    disabled={loading}
                />
                <button
                    class="text-muted hover:text-primary transition-colors p-1"
                    onclick={onClose}
                    aria-label="Close preview"
                >
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                    </svg>
                </button>
            </div>
        </div>

        <!-- Content Area -->
        <div class="flex-1 overflow-auto">
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
                    <MonacoEditor
                        value={textContent}
                        language={getMonacoLanguage(filename)}
                        theme="catppuccin-mocha"
                        readOnly={true}
                    />
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
        </div>

        <!-- Navigation Footer (only show if we have navigation context) -->
        {#if fileList.length > 1 && onNavigate}
            <div class="flex items-center justify-between p-4 border-t border-overlay0">
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
    </div>
</div>