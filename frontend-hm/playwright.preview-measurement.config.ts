import { defineConfig } from "@playwright/test";

import acceptance from "./playwright.docker-acceptance.config";

export default defineConfig({
  ...acceptance,
  testDir: "./tests/e2e-preview-measurement",
  timeout: 300_000,
});
