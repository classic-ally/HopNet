import type { Preview } from '@storybook/svelte-vite';
import '../src/app.css';
import 'uno.css';

const preview: Preview = {
  parameters: {
    backgrounds: {
      default: 'crust',
      values: [
        { name: 'crust', value: '#11111b' },
        { name: 'base', value: '#1e1e2e' },
        { name: 'mantle', value: '#181825' },
      ],
    },
    a11y: { test: 'todo' },
  },
};

export default preview;
