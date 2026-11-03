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

  it.each([200, 401])(
    "cancels an unread late %s response without returning data or navigating",
    async (status) => {
      const controller = new AbortController();
      let respond!: (response: Response) => void;
      globalThis.fetch = rs.fn(
        () =>
          new Promise<Response>((resolve) => {
            respond = resolve;
          }),
      );
      window.history.replaceState({}, "", "/workspace/chats/current-owner");
      const request = apiFetch("/api/threads/old-owner/export", {
        signal: controller.signal,
      });
      controller.abort();
      const cancel = rs.fn();
      respond(new Response(new ReadableStream({ cancel }), { status }));
      await expect(request).rejects.toThrow();
      expect(cancel).toHaveBeenCalledTimes(1);
      expect(window.location.pathname).toBe("/workspace/chats/current-owner");
    },
  );
});

describe("api fetcher CSRF cookies", () => {
  afterEach(() => {
    document.cookie = "theme=; max-age=0; path=/";
    document.cookie = "csrf_token=; max-age=0; path=/";
    rs.unstubAllGlobals();
  });

  it("forwards a malformed CSRF cookie after an unrelated malformed cookie", async () => {
    const sentHeaders: Headers[] = [];
    rs.stubGlobal(
      "fetch",
      rs.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
        sentHeaders.push(new Headers(init?.headers));
        return new Response("ok");
      }),
    );
    // Each assignment sets one cookie; semicolon-separated tails are attributes.
    document.cookie = "theme=%E0; path=/";
    document.cookie = "csrf_token=%gh; path=/";

    expect(document.cookie).toBe("theme=%E0; csrf_token=%gh");

    await apiFetch("http://localhost/api/threads", { method: "POST" });

    expect(sentHeaders).toHaveLength(1);
    expect(sentHeaders[0]?.get("X-CSRF-Token")).toBe("%gh");
  });

  it("still decodes a percent-encoded CSRF cookie", async () => {
    const sentHeaders: Headers[] = [];
    rs.stubGlobal(
      "fetch",
      rs.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
        sentHeaders.push(new Headers(init?.headers));
        return new Response("ok");
      }),
    );
    document.cookie = "csrf_token=cookie%2Dtoken; path=/";

    await apiFetch("http://localhost/api/threads", { method: "POST" });

    expect(sentHeaders[0]?.get("X-CSRF-Token")).toBe("cookie-token");
  });
});
