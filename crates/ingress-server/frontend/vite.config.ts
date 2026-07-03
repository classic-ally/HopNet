import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import UnoCSS from 'unocss/vite';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const dirname = path.dirname(fileURLToPath(import.meta.url));

// The viewer is interim and will fold INTO HopNet. To keep that merge a
// copy-adapt rather than a rewrite, we import HopNet's UI primitives from their
// real source tree via this alias (zero-copy, zero-drift). At fold-in the alias
// is dropped and `$ui/...` imports become native `$lib/...` imports.
//
//   $ui/primitives/Modal.svelte  -> <repo>/frontend/src/lib/primitives/Modal.svelte
//   $ui/Button.svelte            -> <repo>/frontend/src/lib/Button.svelte
const hopnetLib = path.resolve(dirname, '../../../frontend/src/lib');

// Dev talks to the LIVE server on thor (real data, real thumbnails rendered on
// thor's ZFS). No local backend. The one wrinkle is auth: thor's OIDC callback
// redirects to the prod domain, so we can't run the login dance through
// localhost. Instead we borrow a session — log in once at https://photo.bentley.sh,
// copy the `ingress_sid` cookie, and inject it on every proxied request:
//
//     DEV_SID=<cookie value> pnpm dev
//
// tower-sessions' MemoryStore keeps it valid until thor restarts (then re-grab).
const devBackend = 'https://photo.bentley.sh';
const devSid = process.env.DEV_SID ?? '';
const injectCookie = devSid ? { Cookie: `ingress_sid=${devSid}` } : undefined;

export default defineConfig({
  plugins: [UnoCSS(), svelte()],
  resolve: {
    alias: { $ui: hopnetLib },
  },
  server: {
    port: 5173,
    strictPort: true,
    // Allow vite to serve files from HopNet's frontend tree (the aliased primitives).
    fs: { allow: [dirname, hopnetLib] },
    proxy: {
      '/api': { target: devBackend, changeOrigin: true, secure: true, headers: injectCookie },
      '/auth': { target: devBackend, changeOrigin: true, secure: true, headers: injectCookie },
    },
  },
});
