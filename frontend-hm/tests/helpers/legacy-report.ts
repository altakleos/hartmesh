import type { FrontendExtension } from "@/core/extensions/contracts";
import type { LoadedContribution } from "@/core/extensions/registry";

import installed from "../../../backend/extensions/sources/hartmesh-legacy-report/hartmesh_legacy_report/static/index.mjs";

export const legacyReport = installed as FrontendExtension;
export function legacyReportContribution(
  viewerId = "viewer-1",
): LoadedContribution {
  return {
    viewer_id: viewerId,
    namespace: "hartmesh.legacy-report",
    module: "legacy-report.v1",
    entry: "installed-entry",
    title: "Historical report",
    description: "",
    settings: { enabled: true },
    artifact_presentations: [
      {
        id: "report",
        suffixes: [".report.json"],
        source_max_bytes: 16 * 1024 * 1024,
        preview_max_bytes: 1024 * 1024,
        projection_marker: "business-report-v1",
      },
    ],
    extension: legacyReport,
  };
}
