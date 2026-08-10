import type { FilePreviewApi } from './filePreview';

/**
 * In-memory preview content for stories and tests. Nothing in the app imports
 * this, so it tree-shakes out of production bundles.
 */

export interface MockPreviewOptions {
    /// Returned by fetchText.
    text?: string;
    /// Returned by fetchBlob, as-is — a data URI works and needs no network.
    url?: string;
    /// When set, both methods fail with this message instead.
    failWith?: string;
    /// Latency per call, so the loading state is observable.
    latencyMs?: number;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export function mockFilePreviewApi(options: MockPreviewOptions = {}): FilePreviewApi {
    const { text = '', url = '', failWith, latencyMs = 250 } = options;

    async function gate(): Promise<{ ok: false; detail: string } | null> {
        if (latencyMs > 0) await sleep(latencyMs);
        return failWith === undefined ? null : { ok: false, detail: failWith };
    }

    return {
        async fetchText() {
            return (await gate()) ?? { ok: true, text };
        },
        async fetchBlob() {
            return (await gate()) ?? { ok: true, url };
        },
        async download(_path, fallbackFilename) {
            // A data URI, so the anchor click saves a few harmless bytes rather
            // than reaching for the network.
            return (
                (await gate()) ?? {
                    ok: true,
                    url: url || 'data:text/plain;charset=utf-8,' + encodeURIComponent(text),
                    filename: fallbackFilename,
                }
            );
        },
    };
}

export const SAMPLE_CODE = `use std::time::Duration;

/// Stage the target release, then hand control back to the supervisor.
pub async fn stage_and_restart(target: &str) -> Result<(), StageError> {
    let staged = provider.stage(target).await?;
    if staged.version() != target {
        return Err(StageError::WrongBytes {
            expected: target.to_owned(),
            found: staged.version().to_owned(),
        });
    }

    // Exit 75 is the supervisor's cue to restart us into the new generation.
    tracing::info!(target, "activating staged generation");
    provider.activate(&staged)?;
    std::process::exit(EXIT_CODE_RESTART);
}
`;

export const SAMPLE_TEXT = `# Release notes

## 2026.8.2

- Stage upgrades from refs/tags rather than refs/heads
- Put git on the unit's PATH so the flake fetch resolves
- Persist Thread transmit power across restarts

## 2026.8.1

First release carrying the nix upgrade provider. Nodes stage the target
release themselves and cross the epoch boundary unattended.
`;

/// A 1200x800 SVG, inline — a real image with no network involved.
export const SAMPLE_IMAGE_URL =
    'data:image/svg+xml;utf8,' +
    encodeURIComponent(
        `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="800">` +
            `<rect width="1200" height="800" fill="#cba6f7"/>` +
            `<circle cx="600" cy="400" r="220" fill="#89b4fa"/>` +
            `<text x="600" y="420" font-family="sans-serif" font-size="64" fill="#11111b" ` +
            `text-anchor="middle">holiday.png</text></svg>`
    );
