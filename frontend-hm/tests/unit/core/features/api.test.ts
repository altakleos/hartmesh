import { beforeEach, describe, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));

import { fetch } from "@/core/api/fetcher";
import {
  fetchBranding,
  fetchSubagentBatchesCapability,
  fetchWorkspacePresentation,
} from "@/core/features/api";

const mockedFetch = rs.mocked(fetch);

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

beforeEach(() => {
  mockedFetch.mockReset();
});

describe("subagent batch feature capability", () => {
  it("keeps repository and worker availability independent", async () => {
    mockedFetch.mockResolvedValueOnce(
      jsonResponse({
        agents_api: { enabled: true },
        subagent_batches: {
          enabled: false,
          repository_available: true,
          worker_running: false,
          max_running: 3,
        },
      }),
    );

    await expect(fetchSubagentBatchesCapability()).resolves.toEqual({
      repositoryAvailable: true,
      workerRunning: false,
      maxRunning: 3,
    });
  });

  it("falls back to the legacy enabled flag during rolling upgrades", async () => {
    mockedFetch.mockResolvedValueOnce(
      jsonResponse({
        agents_api: { enabled: true },
        subagent_batches: { enabled: true, max_running: 4 },
      }),
    );

    await expect(fetchSubagentBatchesCapability()).resolves.toEqual({
      repositoryAvailable: true,
      workerRunning: true,
      maxRunning: 4,
    });
  });
});

describe("workspace presentation", () => {
  it("reads the profile and the starters the deployment configured", async () => {
    mockedFetch.mockResolvedValueOnce(
      jsonResponse({
        agents_api: { enabled: true },
        ui: {
          profile: "business",
          starters: [
            { id: "review", title: "Monthly review", prompt: "Build it." },
          ],
        },
      }),
    );

    await expect(fetchWorkspacePresentation()).resolves.toEqual({
      profile: "business",
      starters: [
        { id: "review", title: "Monthly review", prompt: "Build it." },
      ],
    });
  });

  it("offers every screen when the Gateway predates the setting", async () => {
    // A rolling upgrade must not take screens away from the people who had
    // them, so an absent block reads as the profile that shows everything.
    mockedFetch.mockResolvedValueOnce(
      jsonResponse({ agents_api: { enabled: true } }),
    );

    await expect(fetchWorkspacePresentation()).resolves.toEqual({
      profile: "developer",
      starters: [],
    });
  });

  it("drops a starter it could not put on a tile", async () => {
    mockedFetch.mockResolvedValueOnce(
      jsonResponse({
        agents_api: { enabled: true },
        ui: {
          profile: "kiosk",
          starters: [
            { id: "ok", title: "Fine", prompt: "Do it." },
            { id: "blank", title: "   ", prompt: "Do it." },
            { id: "missing" },
            "not a starter",
          ],
        },
      }),
    );

    await expect(fetchWorkspacePresentation()).resolves.toEqual({
      // An unknown profile is not a third mode; it is the safe one.
      profile: "developer",
      starters: [{ id: "ok", title: "Fine", prompt: "Do it." }],
    });
  });
});

/**
 * Whose workspace this is comes from the deployment's tenant bundle, after
 * sign-in. The frontend takes the block as reported and never invents a
 * company: a blank name is no name, a colour that is not #rrggbb is no colour,
 * and a Gateway from before the block existed is the product's own.
 */
describe("branding", () => {
  it("reads the company, its colours and whether there is a logo", async () => {
    mockedFetch.mockResolvedValueOnce(
      jsonResponse({
        agents_api: { enabled: true },
        branding: {
          company_name: "Example Services Co.",
          colors: { primary: "#0a6b3d", secondary: "#9ccdb4" },
          logo: true,
        },
      }),
    );

    await expect(fetchBranding()).resolves.toEqual({
      companyName: "Example Services Co.",
      primary: "#0a6b3d",
      secondary: "#9ccdb4",
      hasLogo: true,
    });
  });

  it("is the product's own when the Gateway reports no block", async () => {
    mockedFetch.mockResolvedValueOnce(
      jsonResponse({ agents_api: { enabled: true } }),
    );

    await expect(fetchBranding()).resolves.toEqual({
      companyName: null,
      primary: null,
      secondary: null,
      hasLogo: false,
    });
  });

  it("takes nothing it cannot show", async () => {
    mockedFetch.mockResolvedValueOnce(
      jsonResponse({
        agents_api: { enabled: true },
        branding: {
          company_name: "   ",
          colors: { primary: "green", secondary: 42 },
          logo: "yes",
        },
      }),
    );

    await expect(fetchBranding()).resolves.toEqual({
      companyName: null,
      primary: null,
      secondary: null,
      hasLogo: false,
    });
  });
});
