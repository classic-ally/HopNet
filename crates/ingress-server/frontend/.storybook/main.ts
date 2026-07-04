import type { StorybookConfig } from '@storybook/svelte-vite';
import UnoCSS from 'unocss/vite';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const dirname = path.dirname(fileURLToPath(import.meta.url));
const hopnetLib = path.resolve(dirname, '../../../../frontend/src/lib');

const config: StorybookConfig = {
  stories: ['../src/**/*.stories.@(js|ts|svelte)'],
  addons: ['@storybook/addon-svelte-csf', '@storybook/addon-docs', '@storybook/addon-a11y'],
  // Serve the app's public assets (hopnet-logo.png for LoginPane stories).
  staticDirs: ['../public'],
  framework: { name: '@storybook/svelte-vite', options: {} },
  async viteFinal(config) {
    config.plugins?.push(UnoCSS());
    // Same zero-copy alias as vite.config.ts so component stories resolve
    // HopNet's real primitives.
    config.resolve = config.resolve ?? {};
    config.resolve.alias = { ...(config.resolve.alias as object), $ui: hopnetLib };
    return config;
  },
};
export default config;
