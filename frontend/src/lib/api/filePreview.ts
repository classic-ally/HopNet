import { API_BASE_URL, tokenStore } from '../stores';
import { get } from 'svelte/store';

/**
 * The file preview's content fetch, behind one injectable interface — the same
 * seam pattern as api/setup.ts, and for the same reason: without it the preview
 * can only ever be looked at in its error state, since every content branch
 * (image, PDF, text, editor) is gated behind a real request.
 *
 * Two methods rather than one returning a union, because the component already
 * branches on text-vs-binary and each return type stays honest that way.
 *
 * Blob fetches resolve to an object URL, so the caller keeps ownership of
 * revoking it — FilePreview already does that on file change and unmount.
 */

export type TextResult = { ok: true; text: string } | { ok: false; detail: string };
export type BlobResult = { ok: true; url: string } | { ok: false; detail: string };
export type DownloadResult =
    | { ok: true; url: string; filename: string }
    | { ok: false; detail: string };

export interface FilePreviewApi {
    /** GET /files/<path>, decoded as text. For the text and code previews. */
    fetchText(path: string): Promise<TextResult>;
    /**
     * GET /files/<path> as a blob, retyped to `mimeType` and wrapped in an
     * object URL. The retype matters: the server does not always send a content
     * type the browser will render inline.
     */
    fetchBlob(path: string, mimeType: string): Promise<BlobResult>;
    /**
     * The same bytes, plus the filename to save under — taken from
     * Content-Disposition when the server sends one, else `fallbackFilename`.
     * Triggering the save stays with the caller, which owns the DOM.
     */
    download(path: string, fallbackFilename: string): Promise<DownloadResult>;
}

/// The API wants the path without its leading slash.
function apiPath(path: string): string {
    return path.startsWith('/') ? path.slice(1) : path;
}

async function request(path: string): Promise<Response | { detail: string }> {
    const token = get(tokenStore);
    if (!token) return { detail: 'No authentication token found' };
    try {
        const response = await fetch(`${API_BASE_URL}/files/${apiPath(path)}`, {
            method: 'GET',
            headers: { Authorization: `Bearer ${token}` },
        });
        if (!response.ok) return { detail: `Failed to load preview: ${response.status}` };
        return response;
    } catch (error) {
        return {
            detail: `Network error: ${error instanceof Error ? error.message : 'Unknown error'}`,
        };
    }
}

export const liveFilePreviewApi: FilePreviewApi = {
    async fetchText(path) {
        const result = await request(path);
        if ('detail' in result) return { ok: false, detail: result.detail };
        return { ok: true, text: await result.text() };
    },

    async fetchBlob(path, mimeType) {
        const result = await request(path);
        if ('detail' in result) return { ok: false, detail: result.detail };
        const blob = await result.blob();
        return { ok: true, url: URL.createObjectURL(new Blob([blob], { type: mimeType })) };
    },

    async download(path, fallbackFilename) {
        const result = await request(path);
        if ('detail' in result) return { ok: false, detail: result.detail };

        let filename = fallbackFilename;
        const disposition = result.headers.get('Content-Disposition');
        if (disposition) {
            const match = disposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
            if (match && match[1]) filename = match[1].replace(/['"]/g, '');
        }

        return { ok: true, url: URL.createObjectURL(await result.blob()), filename };
    },
};
