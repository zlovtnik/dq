import { defineConfig } from "vite";
import solid from "vite-plugin-solid";
export default defineConfig({
  plugins: [solid()],
  optimizeDeps: { entries: ["index.html"] },
  build: { outDir: "dist", target: "es2022" },
});
