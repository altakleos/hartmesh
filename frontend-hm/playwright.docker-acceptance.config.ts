import { defineConfig, devices } from "@playwright/test";

const baseURL = process.env.HARTMESH_ACCEPTANCE_URL;
if (!baseURL) throw new Error("Run through scripts/docker_acceptance.py");

export default defineConfig({
  testDir: "./tests/e2e-docker-acceptance",
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  workers: 1,
  timeout: 120_000,
  reporter: "list",
  outputDir: process.env.HARTMESH_ACCEPTANCE_ARTIFACTS,
  use: {
    baseURL,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
