import { afterEach, describe, expect, it, rs } from "@rstest/core";

import {
  AccountExportBusyError,
  AccountExportUnavailableError,
  accountExportPercent,
  discardAccountExport,
  getAccountExport,
  startAccountExport,
  urlOfAccountExportPart,
} from "@/core/account-export";

// What goes in the export, and whose, is the Gateway's
// (backend/tests/test_account_export.py); this page starts it, follows it
// and links to its parts.

function answer(status: number, body?: unknown) {
  return rs.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(body === undefined ? null : JSON.stringify(body), {
        status,
      }),
  );
}

const READY = {
  state: "ready",
  started_at: "2026-09-27T10:30:00+00:00",
  progress: {
    conversations_total: 2,
    conversations_done: 2,
    files_total: 5,
    files_done: 5,
    bytes_total: 100,
    bytes_done: 100,
  },
  parts: [{ number: 1, size: 2048, downloaded: false }],
  skipped: 0,
  expires_at: "2026-09-27T11:30:00+00:00",
};

describe("the account export client", () => {
  afterEach(() => {
    rs.unstubAllGlobals();
  });

  it("sends no person id: every call is /api/account/export", async () => {
    const fetchMock = answer(200, READY);
    rs.stubGlobal("fetch", fetchMock);

    await getAccountExport();
    await startAccountExport();

    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      "/api/account/export",
      "/api/account/export",
    ]);
    expect(fetchMock.mock.calls[1]![1]?.method).toBe("POST");
    expect(urlOfAccountExportPart(2)).toBe("/api/account/export/parts/2");
  });

  it("reads no export as none", async () => {
    rs.stubGlobal("fetch", answer(404, { detail: "No export in progress" }));

    expect(await getAccountExport()).toBeNull();
  });

  it("tells a busy Gateway and an unavailable deployment apart from a failure", async () => {
    rs.stubGlobal("fetch", answer(429, { code: "busy" }));
    await expect(startAccountExport()).rejects.toBeInstanceOf(
      AccountExportBusyError,
    );

    // 503: more than one Gateway process; 403: a sign-in that cannot have one.
    for (const code of [503, 403]) {
      rs.stubGlobal("fetch", answer(code, {}));
      await expect(getAccountExport()).rejects.toBeInstanceOf(
        AccountExportUnavailableError,
      );
    }

    rs.stubGlobal("fetch", answer(500, {}));
    const error = await startAccountExport().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(AccountExportBusyError);
    expect(error).not.toBeInstanceOf(AccountExportUnavailableError);
  });

  it("discards with DELETE, and one already gone is not an error", async () => {
    const fetchMock = answer(404);
    rs.stubGlobal("fetch", fetchMock);

    await discardAccountExport();

    expect(fetchMock.mock.calls[0]![1]?.method).toBe("DELETE");
  });

  it("reports a failed discard: the export is still there", async () => {
    rs.stubGlobal("fetch", answer(500));
    await expect(discardAccountExport()).rejects.toThrow("status 500");
  });
});

describe("accountExportPercent", () => {
  const progress = READY.progress;

  it("moves through both phases: the conversations, then the bytes copied", () => {
    // Transcripts are written first, with nothing copied yet.
    expect(
      accountExportPercent({
        ...progress,
        conversations_done: 1,
        bytes_done: 0,
      }),
    ).toBe(25);
    expect(accountExportPercent({ ...progress, bytes_done: 50 })).toBe(75);
  });

  it("rounds to the nearest whole percent, and never passes 100", () => {
    const bytesOnly = {
      ...progress,
      conversations_total: 0,
      conversations_done: 0,
    };
    expect(
      accountExportPercent({ ...bytesOnly, bytes_total: 3, bytes_done: 1 }),
    ).toBe(33);
    expect(
      accountExportPercent({ ...bytesOnly, bytes_total: 3, bytes_done: 2 }),
    ).toBe(67);
    expect(accountExportPercent({ ...bytesOnly, bytes_done: 150 })).toBe(100);
  });

  it("follows the conversations when there are no files", () => {
    expect(
      accountExportPercent({
        ...progress,
        bytes_total: 0,
        bytes_done: 0,
        conversations_done: 1,
      }),
    ).toBe(50);
  });

  it("starts at nothing", () => {
    expect(
      accountExportPercent({
        conversations_total: 0,
        conversations_done: 0,
        files_total: 0,
        files_done: 0,
        bytes_total: 0,
        bytes_done: 0,
      }),
    ).toBe(0);
  });
});
