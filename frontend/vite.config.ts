/// <reference types="vitest/config" />
import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import UnoCSS from 'unocss/vite';

// Determine backend port based on platform and build mode
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { storybookTest } from '@storybook/addon-vitest/vitest-plugin';
const dirname = typeof __dirname !== 'undefined' ? __dirname : path.dirname(fileURLToPath(import.meta.url));

// More info at: https://storybook.js.org/docs/next/writing-tests/integrations/vitest-addon
function getBackendPort() {
  const isDevelopment = process.env.NODE_ENV === 'development';
  const platform = process.platform;
  let port;
  if (isDevelopment) {
    port = 34634; // Debug mode port
  } else if (platform === 'linux') {
    port = 34633; // Linux port
  } else {
    port = 34632; // Default (macOS) port
  }
  console.log(`🔧 Vite build: Detected platform '${platform}', NODE_ENV '${process.env.NODE_ENV}', using backend port ${port}`);
  return port;
}
const backendPort = getBackendPort();

// https://vite.dev/config/
export default defineConfig({
  plugins: [UnoCSS(), svelte()],
  define: {
    __BACKEND_PORT__: backendPort
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