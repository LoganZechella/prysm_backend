import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3001,
    proxy: {
      '/auth': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
    hmr: {
      overlay: true,
    },
    watch: {
      usePolling: true,
    },
  },
  optimizeDeps: {
    force: true,
  },
})
