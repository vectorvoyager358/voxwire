import { defineConfig } from "vite";

export default defineConfig({
  // GitHub Pages project sites are served from /{repo}/ (see frontend workflow).
  base: process.env.VITE_BASE_PATH ?? "/",
  server: {
    port: 5173,
  },
});
