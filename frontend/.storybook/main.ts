import type { StorybookConfig } from '@storybook/svelte-vite';
import UnoCSS from 'unocss/vite';

const config: StorybookConfig = {
  "stories": [
    "../src/**/*.mdx",
    "../src/**/*.stories.@(js|ts|svelte)"
  ],
  "addons": [
    "@storybook/addon-svelte-csf",
    "@chromatic-com/storybook",
    "@storybook/addon-docs",
    "@storybook/addon-a11y",
    "@storybook/addon-vitest"
  ],
  "framework": {
    "name": "@storybook/svelte-vite",
    "options": {}
  },
  async viteFinal(config, { configType }) {
    // Add UnoCSS plugin to Vite config
    config.plugins?.push(UnoCSS());
    return config;
  }
};
export default config;