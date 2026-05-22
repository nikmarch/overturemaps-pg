import {defineConfig} from 'vite';
import react from '@vitejs/plugin-react';
import {nodePolyfills} from 'vite-plugin-node-polyfills';

// SPA expected to be reverse-proxied under /kepler/. The internal nginx
// (see nginx.conf) listens on :8088 and serves at that prefix; Vite's base
// must match the public mount path so emitted asset URLs resolve.
export default defineConfig({
  base: '/kepler/',
  plugins: [
    react(),
    // Kepler's CJS dist files literally `require("assert")` / "buffer" /
    // "stream" inside browser code paths (dataset-utils, data-utils, etc.).
    // Without shims, the bundler emits an empty module and the first call
    // crashes with `(0, o.default) is not a function`. nodePolyfills() wires
    // up the standard browserify shims for these.
    nodePolyfills({include: ['assert', 'buffer', 'stream', 'util', 'process']}),
  ],
  resolve: {
    // kepler.gl peer-deps mapbox-gl but we want maplibre as the underlying
    // GL renderer (no Mapbox account / token needed). Alias so any import of
    // 'mapbox-gl' resolves to maplibre-gl, which is API-compatible for the
    // subset Kepler uses.
    alias: {
      'mapbox-gl': 'maplibre-gl',
    },
    // npm nests react-palm under each of @kepler.gl/{actions,tasks,table,
    // reducers}, so the bundle ends up with 4 copies. react-palm uses a
    // module-level registry, and 4 registries means task dispatch can't find
    // its handler ("(0,o.default) is not a function" at runtime). Force the
    // bundler to pick one copy. react/react-dom/styled-components are deduped
    // for the same singleton-registry reason (hooks + theme context).
    dedupe: ['react', 'react-dom', 'react-palm', 'styled-components'],
  },
  define: {
    // Some kepler.gl internals reach for process.env.NODE_ENV at runtime.
    'process.env.NODE_ENV': JSON.stringify(process.env.NODE_ENV || 'production'),
  },
  build: {
    target: 'es2020',
    sourcemap: false,
    // kepler.gl is huge; raise the warning threshold so the build log isn't
    // cluttered. Real perf work belongs in code-splitting later.
    chunkSizeWarningLimit: 8000,
  },
  server: {
    port: 5174,
    proxy: {
      '/tiles': 'http://127.0.0.1:7800',
    },
  },
});
