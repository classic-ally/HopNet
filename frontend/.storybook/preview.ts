import type { Preview } from '@storybook/svelte-vite'
// Must mirror src/main.ts, in the same order — see the notes there. app.css only
// *declares* the font family; without the fontsource packages there are no
// @font-face rules behind the declaration, so every story silently falls back to
// system-ui and stops looking like the app.
import '@unocss/reset/tailwind-compat.css'
import '@fontsource-variable/red-hat-display'
import '@fontsource-variable/red-hat-mono'
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