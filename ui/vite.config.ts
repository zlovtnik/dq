import { defineConfig } from "vite";
import solid from "vite-plugin-solid";
export default defineConfig({
  plugins: [solid()],
  optimizeDeps: { entries: ["index.html"] },
  server: { proxy: { "/v1": "http://127.0.0.1:8080" } },
  build: { outDir: "dist", target: "es2022" },
});
