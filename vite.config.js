import { defineConfig } from "vite";

export default defineConfig({
  root: "src",
  build: {
    outDir: "../public/assets/dist",
    emptyOutDir: true,
    rollupOptions: {
      input: {
        main: "src/js/main.js"
      }
    }
  }
});