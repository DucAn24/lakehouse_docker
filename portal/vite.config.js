import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 3002,
    proxy: {
      '/api/catalog': {
        target: 'http://catalog-api:8000',
        rewrite: (path) => path.replace(/^\/api\/catalog/, ''),
        changeOrigin: true,
      },
      '/api/chat': {
        target: 'http://fastapi:8000',
        rewrite: (path) => path.replace(/^\/api\/chat/, ''),
        changeOrigin: true,
      },
    },
  },
})
