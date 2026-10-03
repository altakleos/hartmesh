import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";

import { fetch as apiFetch } from "@/core/api/fetcher";

describe("api fetcher unauthorized redirect", () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
    globalThis.fetch = rs.fn(
      async () => new Response("", { status: 401 }),
    ) as unknown as typeof globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it("ignores a late unauthorized response from an aborted account action", async () => {
    const controller = new AbortController();
    let respond!: (response: Response) => void;
    globalThis.fetch = rs.fn(
      () =>
        new Promise<Response>((resolve) => {
          respond = resolve;
        }),
    );
    window.history.replaceState({}, "", "/workspace/chats/another-account");
    const request = apiFetch("/api/threads/previous-account", {
      method: "DELETE",
      signal: controller.signal,
    });
    controller.abort();
    respond(new Response(null, { status: 401 }));
    await expect(request).rejects.toThrow();
    expect(window.location.pathname).toBe("/workspace/chats/another-account");
  });

  it("returns the caller to the full URL, query string included", async () => {
    window.history.replaceState(
      {},
      "",
      "/artifacts/view?path=%2Fmnt%2Fuser-data%2Foutputs%2Freport.md&thread_id=t-1",
    );

    // The wrapper redirects and then throws UnauthorizedError; the redirect
    // target is what this test is about.
    await expect(
      apiFetch("/api/threads/t-1/artifacts/mnt/user-data/outputs/report.md"),
    ).rejects.toThrow();

    expect(window.location.href).toContain("/login?next=");
    const next = new URL(
      window.location.href,
      "http://localhost",
    ).searchParams.get("next");
    expect(next).toBe(
      "/artifacts/view?path=%2Fmnt%2Fuser-data%2Foutputs%2Freport.md&thread_id=t-1",
    );
  });
});
