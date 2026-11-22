import { defineConfig } from "@playwright/test";

import acceptance from "./playwright.docker-acceptance.config";

export default defineConfig({
  ...acceptance,
  testDir: "./tests/e2e-message-search-measurement",
  timeout: 90_000,
});
