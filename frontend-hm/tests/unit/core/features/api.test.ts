import { beforeEach, describe, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));

import { fetch } from "@/core/api/fetcher";
import {
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
