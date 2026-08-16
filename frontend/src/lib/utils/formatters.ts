// Shared formatting utilities for consistent display across components

/**
 * Format date with standard full timestamp
 * @param dateString ISO date string to format
 * @returns Formatted date string
 */
export function formatDate(dateString: string): string {
    return new Date(dateString).toLocaleString()
}

/**
 * Format file size from bytes to human readable format
 * @param sizeString Size in bytes as string (from u64 serialization)
 * @returns Human readable file size or '-' for folders
 */
export function formatFileSize(sizeString: string | undefined): string {
    if (!sizeString) return '-'

    const size = parseInt(sizeString)
    if (isNaN(size)) return '-'

    if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`
    if (size < 1024 * 1024 * 1024) return `${(size / 1024 / 1024).toFixed(1)} MB`
    return `${(size / 1024 / 1024 / 1024).toFixed(1)} GB`
}

/**
 * Get file extension from filename
 * @param filename Name of the file
 * @returns File extension or empty string
 */
export function getFileExtension(filename: string): string {
    const lastDot = filename.lastIndexOf('.')
    return lastDot > 0 ? filename.substring(lastDot + 1).toLowerCase() : ''
}

/**
 * Get file icon class based on file type and extension
 * @param inodeType Type of inode (File or Folder)
 * @param filename Optional filename for file type detection
 * @param context Context where icon is used - 'list' for simple icons, 'detail' for specific icons
 * @returns CSS class for file icon
 */
export function getFileIcon(inodeType: "File" | "Folder", filename?: string, context: 'list' | 'detail' = 'detail'): string {
    if (inodeType === "Folder") {
        return "i-carbon-folder"
    }

    // For list view, use simple consistent icons
    if (context === 'list') {
        return "i-carbon-document-blank"
    }

    // For detail view (preview pane, thumbnails), use specific icons
    const ext = filename ? getFileExtension(filename) : ''

    // PDF files
    if (ext === 'pdf') {
        return "i-carbon-document-pdf"
    }

    // Code files (matching preview types)
    const codeExtensions = ['js', 'jsx', 'ts', 'tsx', 'py', 'rs', 'go', 'java', 'cpp', 'c', 'h', 'cs', 'php', 'rb', 'sql', 'sh', 'bash', 'ps1', 'dockerfile']
    if (codeExtensions.includes(ext)) {
        return "i-carbon-document-configuration"
    }

    // Text files (matching preview types)
    const textExtensions = ['txt', 'md', 'log', 'yml', 'yaml', 'xml', 'csv', 'json', 'toml', 'ini', 'conf']
    if (textExtensions.includes(ext)) {
        return "i-carbon-document"
    }

    // Media files
    const imageExtensions = ['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp']
    if (imageExtensions.includes(ext)) {
        return "i-carbon-image"
    }

    const videoExtensions = ['mp4', 'mov', 'avi']
    if (videoExtensions.includes(ext)) {
        return "i-carbon-video"
    }

    const audioExtensions = ['mp3', 'wav', 'flac']
    if (audioExtensions.includes(ext)) {
        return "i-carbon-music"
    }

    // Unknown file types
    return "i-carbon-document-unknown"
}

/**
 * Format date for use with CSS container queries
 * Returns an object with different format options that CSS will show/hide
 * @param dateString ISO date string to format
 * @returns Object with different date format options
 */
export function formatDateForContainer(dateString: string) {
    const date = new Date(dateString)

    return {
        dateOnly: date.toLocaleDateString(),
        dateTime: (() => {
            const fullString = date.toLocaleString()
            // Remove seconds while preserving locale format
            return fullString.replace(/:\d{2}(\s*(AM|PM|am|pm))?(\s|$)/, '$1$3')
        })(),
        full: date.toLocaleString()
    }
}

/**
 * Extract filename from full path
 * @param fullPath Full file path string
 * @returns Just the filename portion
 */
export function getFileName(fullPath: string): string {
    if (fullPath === '/') return '/'
    const segments = fullPath.split('/').filter(segment => segment.length > 0)
    return segments[segments.length - 1] || '/'
}

/**
 * Format storage capacity from GB to human readable format with smart units
 * Keeps display to max 3 characters + unit for clean chart display
 * @param gb Storage size in GB
 * @returns Human readable storage capacity (e.g., "900GB", "1.3TB")
 */
export function formatStorageCapacity(gb: number): string {
    if (gb === 0) return '0GB'

    if (gb < 1000) {
        // Under 1TB: show as GB (e.g., "900GB", "50GB")
        return `${Math.floor(gb)}GB`
    } else {
        // 1TB and above: show as TB with 1 decimal if needed (e.g., "1.3TB", "10TB")
        const tb = gb / 1000
        if (tb < 10) {
            // Under 10TB: show 1 decimal place (e.g., "1.3TB", "9.8TB")
            return `${(Math.floor(tb * 10) / 10).toFixed(1)}TB`
        } else {
            // 10TB and above: show whole TB (e.g., "15TB", "100TB")
            return `${Math.floor(tb)}TB`
        }
    }
}