import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'
import UnoCSS from 'unocss/vite'

// Determine backend port based on platform and build mode
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
  plugins: [
    UnoCSS(),
    svelte(),
  ],
  define: {
    __BACKEND_PORT__: backendPort,
  },
})
