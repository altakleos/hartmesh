import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";

import { AccountExportDialog } from "@/components/workspace/account-export-dialog";
import { I18nProvider } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales";

const t = enUS.accountExport;

// The expiry line, whatever its copy and however the time is written.
const MARK = "\u0000";
const [untilHead = "", untilTail = ""] = t.availableUntil(MARK).split(MARK);
const escape = (text: string) => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const AVAILABLE_UNTIL = new RegExp(
  `^${escape(untilHead)}.+${escape(untilTail)}$`,
);

type Status = Record<string, unknown>;

function status(overrides: Status): Status {
  return {
    state: "ready",
    started_at: "2026-09-27T10:30:00+00:00",
    progress: {
      conversations_total: 42,
      conversations_done: 42,
      files_total: 311,
      files_done: 311,
      bytes_total: 1000,
      bytes_done: 1000,
    },
    parts: [{ number: 1, size: 2048, downloaded: false }],
    skipped: 0,
    expires_at: "2026-09-27T11:30:00+00:00",
    ...overrides,
  };
}

function urlOf(input: RequestInfo | URL): string {
  if (typeof input === "string") return input;
  return input instanceof URL ? input.href : input.url;
}

/** The Gateway, as a table of answers by method; records every request. */
function gateway(answers: Record<string, () => Response>) {
  const calls: string[] = [];
  rs.spyOn(globalThis, "fetch").mockImplementation(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      calls.push(`${method} ${urlOf(input)}`);
      const reply = answers[method];
      return reply ? reply() : new Response(null, { status: 500 });
    },
  );
  return calls;
}

function json(body: unknown, statusCode = 200) {
  return () => new Response(JSON.stringify(body), { status: statusCode });
}

function open() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <I18nProvider initialLocale="en-US">
      <QueryClientProvider client={client}>
        <AccountExportDialog open onOpenChange={() => undefined} />
      </QueryClientProvider>
    </I18nProvider>,
  );
}

afterEach(() => {
  cleanup();
  rs.restoreAllMocks();
});

describe("Download all my data", () => {
  it("says what it holds and whose, then starts it", async () => {
    const calls = gateway({
      GET: json({ detail: "No export in progress" }, 404),
      POST: json(
        status({
          state: "building",
          parts: [],
          progress: {
            conversations_total: 42,
            conversations_done: 7,
            files_total: 311,
            files_done: 0,
            bytes_total: 1000,
            bytes_done: 0,
          },
        }),
        202,
      ),
    });
    open();

    expect(screen.getByText(t.description)).toBeTruthy();
    expect(screen.getByText(t.onlyYours)).toBeTruthy();
    fireEvent.click(await screen.findByRole("button", { name: t.start }));

    expect(await screen.findByText(t.preparing)).toBeTruthy();
    expect(
      screen.getByText(
        `${t.conversationsProgress(7, 42)} · ${t.filesProgress(0, 311)}`,
      ),
    ).toBeTruthy();
    expect(calls).toContain("POST /api/account/export");
  });

  it("stops one being prepared", async () => {
    const calls = gateway({
      GET: json(status({ state: "building", parts: [] })),
      DELETE: () => new Response(null, { status: 204 }),
    });
    open();

    fireEvent.click(await screen.findByRole("button", { name: t.stop }));

    await waitFor(() => expect(calls).toContain("DELETE /api/account/export"));
    expect(await screen.findByRole("button", { name: t.start })).toBeTruthy();
  });

  it("links each part for the browser to save, and says what was left out", async () => {
    gateway({
      GET: json(
        status({
          parts: [
            { number: 1, size: 2 * 1024 ** 3, downloaded: true },
            { number: 2, size: 1024 ** 2, downloaded: false },
          ],
          skipped: 3,
        }),
      ),
    });
    open();

    const first = await screen.findByRole("link", {
      name: t.downloadPart(1, 2),
    });
    const second = screen.getByRole("link", { name: t.downloadPart(2, 2) });
    expect(first.getAttribute("href")).toBe("/api/account/export/parts/1");
    expect(second.getAttribute("href")).toBe("/api/account/export/parts/2");
    expect(second.hasAttribute("download")).toBe(true);
    expect(screen.getByText(t.multiPart)).toBeTruthy();
    expect(screen.getByText(t.skipped(3))).toBeTruthy();
    expect(screen.getByText(t.downloaded)).toBeTruthy();
    expect(screen.getByText("2 GiB")).toBeTruthy();
  });

  it("offers one download for a single part, and says when it is downloaded", async () => {
    gateway({ GET: json(status({ state: "downloaded" })) });
    open();

    expect(
      (await screen.findByRole("link", { name: t.download })).getAttribute(
        "href",
      ),
    ).toBe("/api/account/export/parts/1");
    expect(screen.getByText(t.allDownloaded)).toBeTruthy();
  });

  it("names a full disk, and tries again", async () => {
    const calls = gateway({
      GET: json(
        status({
          state: "failed",
          parts: [],
          error: { code: "no_space", detail: "not enough free space" },
        }),
      ),
      POST: json(status({ state: "building", parts: [] }), 202),
    });
    open();

    expect(await screen.findByText(t.noSpace)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: t.tryAgain }));

    expect(await screen.findByText(t.preparing)).toBeTruthy();
    expect(calls).toContain("POST /api/account/export");
  });

  it("says when the Gateway is busy with other people's", async () => {
    gateway({
      GET: json({ detail: "No export in progress" }, 404),
      POST: json({ detail: "busy", code: "busy" }, 429),
    });
    open();

    fireEvent.click(await screen.findByRole("button", { name: t.start }));

    expect(await screen.findByText(t.busy)).toBeTruthy();
  });

  it("says when the deployment cannot prepare one", async () => {
    gateway({ GET: json({ detail: "unavailable" }, 503) });
    open();

    expect(await screen.findByText(t.unavailable)).toBeTruthy();
    expect(screen.queryByRole("button", { name: t.start })).toBeNull();
  });
});

describe("Download all my data, each moment", () => {
  it("says a deployment that cannot prepare one when starting", async () => {
    gateway({
      GET: json({ detail: "No export in progress" }, 404),
      POST: json({ detail: "unavailable" }, 503),
    });
    open();
    fireEvent.click(await screen.findByRole("button", { name: t.start }));
    expect(await screen.findByText(t.unavailable)).toBeTruthy();
  });

  it("says it could not check, when reading the export fails", async () => {
    gateway({ GET: json({ detail: "boom" }, 500) });
    open();
    expect(await screen.findByText(t.loadFailed)).toBeTruthy();
    expect(screen.queryByText(t.unavailable)).toBeNull();
  });

  it("names a failure that is not a full disk as a failure", async () => {
    gateway({
      GET: json(
        status({
          state: "failed",
          parts: [],
          error: { code: "failed", detail: null },
        }),
      ),
    });
    open();
    expect(await screen.findByText(t.failed)).toBeTruthy();
    expect(screen.queryByText(t.noSpace)).toBeNull();
  });

  it("says why trying again was refused, over the earlier failure", async () => {
    gateway({
      GET: json(
        status({
          state: "failed",
          parts: [],
          error: { code: "no_space", detail: null },
        }),
      ),
      POST: json({ detail: "busy" }, 429),
    });
    open();
    fireEvent.click(await screen.findByRole("button", { name: t.tryAgain }));
    expect(await screen.findByText(t.busy)).toBeTruthy();
    expect(screen.queryByText(t.noSpace)).toBeNull();
  });

  it("counts only conversations when there are no files, and shows the percent", async () => {
    gateway({
      GET: json(
        status({
          state: "building",
          parts: [],
          progress: {
            conversations_total: 4,
            conversations_done: 1,
            files_total: 0,
            files_done: 0,
            bytes_total: 0,
            bytes_done: 0,
          },
        }),
      ),
    });
    open();
    expect(await screen.findByText(t.conversationsProgress(1, 4))).toBeTruthy();
    // The shared Progress keeps `value` off the Radix root (no aria-valuenow),
    // so the only trace of the percent is the indicator's offset.
    const indicator = screen
      .getByRole("progressbar")
      .querySelector<HTMLElement>('[data-slot="progress-indicator"]')!;
    expect(indicator.style.transform).toBe("translateX(-75%)");
  });

  it("says a single ready part is ready, with no talk of parts, nothing left out, and until when", async () => {
    gateway({ GET: json(status({})) });
    open();
    expect(await screen.findByText(t.ready)).toBeTruthy();
    expect(screen.queryByText(t.allDownloaded)).toBeNull();
    expect(screen.queryByText(t.multiPart)).toBeNull();
    expect(screen.queryByText(/could not be included/)).toBeNull();
    expect(screen.getByText(AVAILABLE_UNTIL)).toBeTruthy();
    expect(screen.queryByText(t.keptWhileDownloading)).toBeNull();
  });

  it("says it is kept while a part downloads when it has no expiry", async () => {
    gateway({ GET: json(status({ expires_at: null })) });
    open();
    expect(await screen.findByText(t.keptWhileDownloading)).toBeTruthy();
    expect(screen.queryByText(AVAILABLE_UNTIL)).toBeNull();
  });

  it("gives no expiry once everything is downloaded", async () => {
    gateway({ GET: json(status({ state: "downloaded" })) });
    open();
    expect(await screen.findByText(t.allDownloaded)).toBeTruthy();
    expect(screen.queryByText(AVAILABLE_UNTIL)).toBeNull();
    expect(screen.queryByText(t.keptWhileDownloading)).toBeNull();
  });

  it("marks exactly the parts already downloaded", async () => {
    gateway({
      GET: json(
        status({
          parts: [
            { number: 1, size: 10, downloaded: true },
            { number: 2, size: 10, downloaded: false },
          ],
        }),
      ),
    });
    open();
    const first = await screen.findByRole("link", {
      name: t.downloadPart(1, 2),
    });
    const second = screen.getByRole("link", { name: t.downloadPart(2, 2) });
    const firstRow = first.closest("li")!;
    const secondRow = second.closest("li")!;
    expect(within(firstRow).queryByText(t.downloaded)).not.toBeNull();
    expect(within(secondRow).queryByText(t.downloaded)).toBeNull();
    // The one still to download is the one that stands out.
    expect(first.getAttribute("data-variant")).toBe("outline");
    expect(second.getAttribute("data-variant")).toBe("default");
  });

  it("deletes a ready one now", async () => {
    const calls = gateway({
      GET: json(status({})),
      DELETE: () => new Response(null, { status: 204 }),
    });
    open();
    fireEvent.click(await screen.findByRole("button", { name: t.deleteNow }));
    await waitFor(() => expect(calls).toContain("DELETE /api/account/export"));
    expect(await screen.findByRole("button", { name: t.start })).toBeTruthy();
  });
});

describe("Download all my data, when something goes wrong", () => {
  it("offers to check again when reading the export fails, and does", async () => {
    let fail = true;
    gateway({
      GET: () =>
        fail
          ? new Response("{}", { status: 500 })
          : new Response(JSON.stringify(status({})), { status: 200 }),
    });
    open();

    expect(await screen.findByText(t.loadFailed)).toBeTruthy();
    fail = false;
    fireEvent.click(screen.getByRole("button", { name: t.tryAgain }));

    expect(await screen.findByText(t.ready)).toBeTruthy();
  });

  it("offers no retry where the deployment cannot prepare one", async () => {
    gateway({ GET: json({ detail: "sign-in only" }, 403) });
    open();

    expect(await screen.findByText(t.unavailable)).toBeTruthy();
    expect(screen.queryByRole("button", { name: t.tryAgain })).toBeNull();
  });

  it("says a download that was deleted meanwhile is gone", async () => {
    let gone = false;
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    gateway({
      GET: () =>
        gone
          ? new Response("{}", { status: 404 })
          : new Response(JSON.stringify(status({})), { status: 200 }),
    });
    render(
      <I18nProvider initialLocale="en-US">
        <QueryClientProvider client={client}>
          <AccountExportDialog open onOpenChange={() => undefined} />
        </QueryClientProvider>
      </I18nProvider>,
    );
    expect(await screen.findByText(t.ready)).toBeTruthy();

    gone = true;
    await client.refetchQueries();

    expect(await screen.findByText(t.gone)).toBeTruthy();
    expect(screen.getByRole("button", { name: t.start })).toBeTruthy();
  });

  it("says when deleting failed, and keeps the download", async () => {
    gateway({
      GET: json(status({})),
      DELETE: () => new Response("{}", { status: 500 }),
    });
    open();

    fireEvent.click(await screen.findByRole("button", { name: t.deleteNow }));

    expect(await screen.findByText(t.deleteFailed)).toBeTruthy();
    expect(screen.getByRole("link", { name: t.download })).toBeTruthy();
  });

  it("says it can take a while and the window can be closed", async () => {
    gateway({ GET: json({ detail: "No export in progress" }, 404) });
    open();

    expect(await screen.findByText(t.closeAnytime)).toBeTruthy();
  });

  it("counts nothing before the Gateway has found what to export", async () => {
    gateway({
      GET: json(
        status({
          state: "building",
          parts: [],
          progress: {
            conversations_total: 0,
            conversations_done: 0,
            files_total: 0,
            files_done: 0,
            bytes_total: 0,
            bytes_done: 0,
          },
        }),
      ),
    });
    open();

    expect(await screen.findByText(t.preparing)).toBeTruthy();
    expect(screen.queryByText(/Conversations:/)).toBeNull();
  });
});
