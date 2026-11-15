import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

export default defineConfig({
  base: './',
  plugins: [svelte()],
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:8000',
      '/demo.bin': 'http://127.0.0.1:8000',
    },
  },
  build: { target: 'es2022' },
});
