/// <reference types="vitest/config" />
import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import UnoCSS from 'unocss/vite';

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { storybookTest } from '@storybook/addon-vitest/vitest-plugin';
const dirname = typeof __dirname !== 'undefined' ? __dirname : path.dirname(fileURLToPath(import.meta.url));

// Dev-server proxy target. The frontend itself uses relative URLs in every
// mode (Tauri webview, browser served from axum static, and vite dev) — the
// proxy below only matters for `pnpm dev`, where vite serves the SPA and
// forwards API routes to a separately-running headless backend.
const devProxyTarget = 'http://localhost:34632';

// https://vite.dev/config/
export default defineConfig({
  plugins: [UnoCSS(), svelte()],
  server: {
    proxy: {
      // Proxy all API requests to the Rust backend in dev mode.
      '/api': {
        target: devProxyTarget,
        changeOrigin: true,
      }
    }
  },
  test: {
    projects: [{
      // Logic tests: no real browser, so they run anywhere `pnpm` runs.
      //
      // happy-dom is not decoration. Vitest picks its transform from the
      // environment — `node` gets the SSR transform, which compiles runes
      // against Svelte's *server* runtime, where $effect is a no-op and a
      // $derived computes once and never invalidates again. Tests then read
      // stale values and pass or fail for reasons that have nothing to do
      // with the code. A DOM environment selects the web transform and the
      // client runtime, which is what makes TableState testable at all.
      extends: true,
      test: {
        name: 'unit',
        environment: 'happy-dom',
        include: ['src/**/*.test.ts'],
      },
      resolve: {
        conditions: ['browser'],
      },
      ssr: {
        resolve: {
          conditions: ['browser'],
        },
      },
    }, {
      extends: true,
      plugins: [
      // The plugin will run tests for the stories defined in your Storybook config
      // See options at: https://storybook.js.org/docs/next/writing-tests/integrations/vitest-addon#storybooktest
      storybookTest({
        configDir: path.join(dirname, '.storybook')
      })],
      test: {
        name: 'storybook',
        browser: {
          enabled: true,
          headless: true,
          provider: 'playwright',
          instances: [{
            browser: 'chromium',
            // Playwright's bundled chromium has no usable shared libraries on
            // NixOS; CHROME_BIN (set by the dev shell) points at the nixpkgs
            // build instead. Undefined elsewhere, where the bundle is fine.
            launch: { executablePath: process.env.CHROME_BIN },
          }]
        },
        setupFiles: ['.storybook/vitest.setup.ts']
      }
    }]
  }
});