import { defineConfig } from 'vite';
import solid from 'vite-plugin-solid';
export default defineConfig({
  plugins:[solid()], base:'/dq-ui/',
  build:{outDir:'dist/embedded',emptyOutDir:true,target:'es2022',rollupOptions:{input:'embedded.html'}},
});
