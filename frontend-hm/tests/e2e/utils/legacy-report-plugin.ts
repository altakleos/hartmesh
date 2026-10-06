import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import type { Page } from "@playwright/test";

const directory = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../../../../backend/extensions/sources/hartmesh-legacy-report/hartmesh_legacy_report/static",
);
const revision = "c".repeat(64);

/** Serve the real provider package through the production installed-assets transport. */
export async function installLegacyReportPlugin(page: Page, enabled = true) {
  await page.route("**/api/plugins", (route) =>
    route.fulfill({
      json: [
        {
          viewer_id: "default",
          namespace: "hartmesh.legacy-report",
          module: "legacy-report.v1",
          entry: `/api/plugins/hartmesh.legacy-report/assets/${revision}/static/index.mjs`,
          transport: "assets-v1",
          title: "Historical report",
          description: "",
          settings: { enabled },
          backend_actions: [],
          artifact_presentations: [
            {
              id: "report",
              suffixes: [".report.json"],
              source_max_bytes: 16 * 1024 * 1024,
              preview_max_bytes: 1024 * 1024,
              projection_marker: "business-report-v1",
            },
          ],
        },
      ],
    }),
  );
  await page.route(
    `**/api/plugins/hartmesh.legacy-report/assets/${revision}/static/*.mjs`,
    (route) => {
      const filename = new URL(route.request().url()).pathname
        .split("/")
        .at(-1)!;
      if (
        !["index.mjs", "report.mjs", "format.mjs", "paths.mjs"].includes(
          filename,
        )
      )
        return route.fulfill({ status: 404 });
      return route.fulfill({
        contentType: "text/javascript",
        body: readFileSync(resolve(directory, filename), "utf-8"),
      });
    },
  );
}
