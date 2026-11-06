import { defineConfig, devices } from "@playwright/test";

const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? "http://localhost:3000";
const skipWebServer = process.env.PLAYWRIGHT_SKIP_WEB_SERVER === "1";
const productGatewayPort = process.env.E2E_PRODUCT_GATEWAY_PORT ?? "3019";
const productGatewayURL = `http://127.0.0.1:${productGatewayPort}`;

export default defineConfig({
  testDir: "./tests/e2e",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: process.env.CI ? "github" : "html",
  timeout: 30_000,

  use: {
    baseURL,
    locale: "en-US",
    trace: "on-first-retry",
  },

  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],

  webServer: skipWebServer
    ? undefined
    : [
        {
          command: "node tests/fixtures/product-gateway.mjs",
          url: `${productGatewayURL}/api/v1/auth/product-identity`,
          reuseExistingServer: !process.env.CI,
          env: { E2E_PRODUCT_GATEWAY_PORT: productGatewayPort },
        },
        {
          command:
            "./node_modules/.bin/next build && ./node_modules/.bin/next start",
          url: baseURL,
          reuseExistingServer: !process.env.CI,
          timeout: 120_000,
          env: {
            SKIP_ENV_VALIDATION: "1",
            DEER_FLOW_AUTH_DISABLED: "1",
            DEER_FLOW_INTERNAL_GATEWAY_BASE_URL: productGatewayURL,
          },
        },
      ],
});
