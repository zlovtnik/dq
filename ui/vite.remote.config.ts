import { defineConfig } from "vite";
import solid from "vite-plugin-solid";
export default defineConfig({
  plugins: [solid()],
  build: {
    outDir: "remote-build",
    target: "es2022",
    lib: {
      entry: "remote/index.tsx",
      formats: ["es"],
      fileName: () => "remote.js",
    },
    rollupOptions: { output: { inlineDynamicImports: true } },
  },
});
