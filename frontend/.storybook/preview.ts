import type { Preview } from '@storybook/svelte-vite'
import '../src/app.css'
import 'uno.css'

const preview: Preview = {
  parameters: {
    controls: {
      matchers: {
       color: /(background|color)$/i,
       date: /Date$/i,
      },
    },

    backgrounds: {
      default: 'dark',
      values: [
        {
          name: 'dark',
          value: '#1e1e2e', // matches your base color
        },
        {
          name: 'light',
          value: '#ffffff',
        },
      ],
    },

    a11y: {
      // 'todo' - show a11y violations in the test UI only
      // 'error' - fail CI on a11y violations
      // 'off' - skip a11y checks entirely
      test: 'todo'
    }
  },
};

export default preview;