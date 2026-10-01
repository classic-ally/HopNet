// Captures the hopnet.app screenshots from the `Website/*` Storybook stories.
//
//   CHROME_BIN=/path/to/chromium pnpm shots
//
// Serves the built storybook-static/ locally, renders each story at a fixed
// viewport, clock, locale and timezone, and writes <slot>.webp (or .png when
// cwebp is missing) plus the projection code excerpt and a manifest into
// website-shots/. The website repo copies that directory in verbatim.

import { spawnSync } from 'node:child_process';
import { createReadStream, existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import { extname, join, resolve } from 'node:path';
import { chromium, type Page } from 'playwright';

const FRONTEND = resolve(import.meta.dirname, '..');
const REPO = resolve(FRONTEND, '..');
const STATIC = join(FRONTEND, 'storybook-static');
const OUT = join(FRONTEND, 'website-shots');

const VIEWPORT = { width: 1280, height: 800 };
const FIXED_TIME = new Date('2026-06-15T10:00:00Z');

interface Shot {
    slot: string;
    story: string;
    /** Resolves once the story shows what the screenshot is meant to show. */
    ready: (page: Page) => Promise<unknown>;
}

const SHOTS: Shot[] = [
    {
        slot: 'photos-gallery',
        story: 'website--photos-gallery',
        ready: (page) =>
            page.waitForFunction(() => {
                const loaded = [...document.images].filter((img) => img.src.startsWith('blob:') && img.complete && img.naturalWidth > 0);
                return loaded.length >= 12;
            }),
    },
    {
        slot: 'web-files',
        story: 'website--files',
        ready: (page) => page.getByText('Household budget 2026.ods').waitFor(),
    },
    {
        slot: 'share-dialog',
        story: 'website--sharing',
        ready: (page) => page.locator('button.bg-mauve\\/20', { hasText: 'priya' }).waitFor(),
    },
    {
        slot: 'takeout',
        story: 'website--export',
        ready: (page) => page.getByText('Ready', { exact: true }).first().waitFor(),
    },
    {
        slot: 'web-login',
        story: 'website--sign-in',
        ready: (page) => page.getByRole('button', { name: 'Log in' }).first().waitFor(),
    },
];

// Code shown on the website's "build your own app" tile, in this order.
const EXCERPTS = ['hopnet-takeout/src/lib.rs', 'src/projections.rs'];

const MIME: Record<string, string> = {
    '.html': 'text/html',
    '.js': 'text/javascript',
    '.mjs': 'text/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.svg': 'image/svg+xml',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.woff2': 'font/woff2',
};

function serve(root: string): Promise<{ server: Server; origin: string }> {
    const server = createServer((req, res) => {
        const pathname = decodeURIComponent(new URL(req.url ?? '/', 'http://x').pathname);
        let file = join(root, pathname);
        if (!file.startsWith(root) || !existsSync(file) || statSync(file).isDirectory()) {
            // The shell rewrites the iframe URL to app routes; only real files exist.
            file = join(root, 'iframe.html');
        }
        res.setHeader('Content-Type', MIME[extname(file)] ?? 'application/octet-stream');
        createReadStream(file).pipe(res);
    });
    return new Promise((done) =>
        server.listen(0, '127.0.0.1', () => {
            const { port } = server.address() as { port: number };
            done({ server, origin: `http://127.0.0.1:${port}` });
        }),
    );
}

function excerpt(relative: string): string {
    const lines = readFileSync(join(REPO, relative), 'utf8').split('\n');
    const start = lines.findIndex((l) => l.includes('website-excerpt:start'));
    const end = lines.findIndex((l) => l.includes('website-excerpt:end'));
    if (start < 0 || end <= start) throw new Error(`${relative}: missing website-excerpt markers`);
    return lines.slice(start + 1, end).join('\n');
}

function toWebp(png: string, webp: string): boolean {
    const result = spawnSync('cwebp', ['-quiet', '-q', '90', '-m', '6', png, '-o', webp]);
    if (result.error || result.status !== 0) return false;
    rmSync(png);
    return true;
}

async function main() {
    const executablePath = process.env.CHROME_BIN;
    if (!executablePath) throw new Error('Set CHROME_BIN to a chromium binary (e.g. $(nix build nixpkgs#chromium --print-out-paths)/bin/chromium)');
    if (!existsSync(join(STATIC, 'iframe.html'))) throw new Error('storybook-static/ is missing; run `pnpm build-storybook` first');

    rmSync(OUT, { recursive: true, force: true });
    mkdirSync(OUT, { recursive: true });

    const { server, origin } = await serve(STATIC);
    const browser = await chromium.launch({ executablePath });
    const written: string[] = [];
    try {
        for (const shot of SHOTS) {
            const context = await browser.newContext({
                viewport: VIEWPORT,
                deviceScaleFactor: 2,
                locale: 'en-US',
                timezoneId: 'UTC',
                reducedMotion: 'reduce',
                colorScheme: 'dark',
            });
            const page = await context.newPage();
            await page.clock.setFixedTime(FIXED_TIME);
            page.on('console', (msg) => {
                if (msg.text().includes('[website mock]')) console.warn(`  ${shot.slot}: ${msg.text()}`);
            });

            await page.goto(`${origin}/iframe.html?id=${shot.story}&viewMode=story`);
            await shot.ready(page);
            await page.evaluate(() => document.fonts.ready);
            // Let transitions and late layout settle before the capture.
            await page.waitForTimeout(600);

            const png = join(OUT, `${shot.slot}.png`);
            await page.screenshot({ path: png, animations: 'disabled', caret: 'hide' });
            const file = toWebp(png, join(OUT, `${shot.slot}.webp`)) ? `${shot.slot}.webp` : `${shot.slot}.png`;
            written.push(file);
            console.log(`captured ${file}`);
            await context.close();
        }
    } finally {
        await browser.close();
        server.close();
    }

    writeFileSync(join(OUT, 'projection-code.rs'), EXCERPTS.map(excerpt).join('\n\n') + '\n');
    written.push('projection-code.rs');

    const commit = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: REPO, encoding: 'utf8' }).stdout.trim();
    writeFileSync(join(OUT, 'manifest.json'), JSON.stringify({ commit, viewport: VIEWPORT, files: written }, null, 2) + '\n');
    console.log(`wrote ${written.length} files to ${OUT}`);
}

await main();
