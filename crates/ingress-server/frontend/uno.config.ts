// The viewer shares HopNet's exact design tokens (Catppuccin Mocha) by
// re-exporting its UnoCSS config. We only augment `content.filesystem`: UnoCSS
// scans source files to find used utility classes, and the aliased primitives
// live OUTSIDE this tree — without adding HopNet's lib to the scan, classes used
// only inside Modal/Toolbar/etc. would never be generated.
import base from '../../../frontend/uno.config';

export default {
  ...base,
  content: {
    ...base.content,
    filesystem: [
      'src/**/*.{svelte,ts,js}',
      '../../../frontend/src/lib/**/*.{svelte,ts,js}',
    ],
  },
};
