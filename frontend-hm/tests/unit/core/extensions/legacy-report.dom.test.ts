import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { afterEach, describe, expect, it, rs } from "@rstest/core";

import type { ArtifactSurfaceContext } from "@/core/extensions/contracts";
import { installedFileCollection } from "@/core/extensions/filing";

import {
  legacyReport,
  legacyReportContribution,
} from "../../../helpers/legacy-report";

const raw = readFileSync(
  resolve("tests/fixtures/business-report/2026-08-business-review.report.json"),
  "utf-8",
);
const path = "/mnt/user-data/outputs/results/month.report.json";

function mount(options: Partial<ArtifactSurfaceContext> = {}) {
  const root = document.createElement("div");
  document.body.append(root);
  const controller = new AbortController();
  const context: ArtifactSurfaceContext = {
    namespace: "hartmesh.legacy-report",
    locale: "en-US",
    settings: { enabled: true },
    signal: controller.signal,
    theme: "light",
    callBackend: rs.fn(),
    artifact: {
      filepath: path,
      threadId: "thread-1",
      revision: "a".repeat(64),
      content: raw,
      projected: false,
      presented: [path],
    },
    loadRaster: rs.fn(async () => "blob:chart"),
    ...options,
  };
  const surface = legacyReport.artifacts![0]!;
  if (surface.kind !== "native") throw new Error("Expected native adapter");
  const mounted = surface.mount(root, context);
  return { root, mounted, controller, context };
}

afterEach(() => document.body.replaceChildren());

describe("independent historical report module", () => {
  it("draws historical figures with its own styles and returns generic file controls", () => {
    const { root, mounted } = mount();
    expect(root.querySelector("h2")?.textContent).toBe(
      "August 2026 Business Review",
    );
    expect(
      root.querySelector('[data-testid="business-report-kpi-value"]')
        ?.textContent,
    ).toBe("$74,702.61");
    expect(root.querySelector("style")?.textContent).toContain(
      "minmax(min(100%,10rem),1fr)",
    );
    expect(
      root.querySelector('[data-testid="business-report-checks-line"]')
        ?.textContent,
    ).toContain("Totals match your file");
    expect(mounted.files).toEqual({
      exports: [
        { path: "month.pdf", label: "PDF" },
        { path: "month.docx", label: "Word" },
        { path: "month.xlsx", label: "Excel" },
      ],
      collection: "Reports",
    });
    mounted.dispose();
    expect(root.childNodes).toHaveLength(0);
  });

  it("keeps charts load-bounded and contains missing charts", async () => {
    const loader = rs.fn(async () => {
      throw new Error("missing");
    });
    const { root } = mount({ loadRaster: loader });
    await Promise.resolve();
    await Promise.resolve();
    expect(loader).toHaveBeenCalledWith("charts/revenue_by_period.png");
    expect(root.querySelectorAll("figure")).toHaveLength(0);
    expect(root.querySelector("h2")).not.toBeNull();
  });

  it("does not attach late chart URLs after disposal", async () => {
    let resolveChart!: (url: string) => void;
    const { root, mounted } = mount({
      loadRaster: () =>
        new Promise((resolve) => {
          resolveChart = resolve;
        }),
    });
    const image = root.querySelector("img")!;
    mounted.dispose();
    resolveChart("blob:retired");
    await Promise.resolve();
    expect(image.getAttribute("src")).toBeNull();
    expect(root.childNodes).toHaveLength(0);
  });

  it("owns its translations and refuses malformed reports", () => {
    const { root } = mount({ locale: "zh-CN", theme: "dark" });
    expect(root.textContent).toContain("第 1 稿");
    expect(root.querySelector(".dark")).not.toBeNull();
    expect(() =>
      mount({
        artifact: {
          filepath: path,
          threadId: "thread-1",
          revision: "a".repeat(64),
          content: "{}",
          projected: false,
          presented: [],
        },
      }),
    ).toThrow();
  });

  it("files only presented report companions and the flat legacy personal folder", () => {
    const entries = [legacyReportContribution()];
    const context = {
      filepath: path.replace(".report.json", ".pdf"),
      destination: "shared" as const,
      presented: [path],
    };
    expect(installedFileCollection(entries, context)).toBe("Reports");
    expect(
      installedFileCollection(entries, { ...context, presented: [] }),
    ).toBeUndefined();
    expect(
      installedFileCollection(entries, {
        ...context,
        filepath: "/mnt/user-data/files/Reports/month.pdf",
        presented: [],
      }),
    ).toBe("Reports");
    for (const filepath of [
      "/mnt/user-data/files/Private/month.pdf",
      "/mnt/user-data/files/Reports/nested/month.pdf",
    ]) {
      expect(
        installedFileCollection(entries, {
          ...context,
          filepath,
          presented: [],
        }),
      ).toBeUndefined();
    }
  });
});
