import { defineConfig } from "@playwright/test";

import replay from "./playwright.real-backend.config";

const servers = Array.isArray(replay.webServer)
  ? replay.webServer
  : replay.webServer
    ? [replay.webServer]
    : [];

// Exercise the same-origin proxy and SSR against a real, auth-enabled Gateway.
// The replay runner provides an isolated database; no external provider is used.
export default defineConfig({
  ...replay,
  testDir: "./tests/e2e-gateway-auth",
  webServer: servers.map((server) => ({
    ...server,
    reuseExistingServer: false,
    env: { ...server.env, DEER_FLOW_AUTH_DISABLED: "0" },
  })),
});
