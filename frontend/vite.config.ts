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
      // The SPA only serves / (index.html) and /assets/*; everything else is an API route.
      '^/(login|logout|setup|nodes|files|fragments|users|shares|metrics|devices|consensus|takeout|admin|maintenance|diagnostics|debug|validators|integrations|test)': {
        target: devProxyTarget,
        changeOrigin: true,
      }
    }
  },
  test: {
    projects: [{
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
            browser: 'chromium'
          }]
        },
        setupFiles: ['.storybook/vitest.setup.ts']
      }
    }]
  }
});