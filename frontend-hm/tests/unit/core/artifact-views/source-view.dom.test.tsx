import { afterEach, expect, rs, test } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";

import { useSourceView } from "@/core/artifact-views/source-view";

const identity = rs.hoisted(() => ({ viewer: "viewer-1" }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: identity.viewer } }),
}));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("@/core/static-mode", () => ({ isStaticWebsiteOnly: () => false }));
const SOURCE = "/mnt/user-data/outputs/results/source.json";
const VIEW = "/mnt/user-data/outputs/results/result.view.json";
const presented = [SOURCE, VIEW];
const response = (title: string) =>
  new Response(
    JSON.stringify({
      format: "hartmesh.artifact-view",
      version: 1,
      title,
      primary_source: { path: "source.json" },
      blocks: [],
      exports: [],
    }),
    { headers: { ETag: `"${"a".repeat(64)}"` } },
  );

function Host({
  settled = true,
  paths = presented,
}: {
  settled?: boolean;
  paths?: readonly string[];
}) {
  const { sourceView } = useSourceView({
    filepath: SOURCE,
    presented: paths,
    threadId: "thread-1",
    enabled: true,
    runSettled: settled,
  });
  return <p>{sourceView?.view.title ?? "Ordinary source"}</p>;
}
afterEach(() => {
  cleanup();
  rs.restoreAllMocks();
  identity.viewer = "viewer-1";
});

test("unused source-view documents are evicted instead of caching historical results", async () => {
  rs.spyOn(globalThis, "fetch").mockResolvedValue(response("Related result"));
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const { unmount } = render(
    <QueryClientProvider client={client}>
      <Host />
    </QueryClientProvider>,
  );
  await waitFor(() => expect(screen.getByText("Related result")).toBeDefined());
  unmount();
  await waitFor(() =>
    expect(
      client.getQueryCache().findAll({ queryKey: ["artifact-source-view"] }),
    ).toHaveLength(0),
  );
});

test("changing viewer aborts a delayed old document and releases its eventual unread body", async () => {
  let finish!: (response: Response) => void;
  const fetch = rs
    .spyOn(globalThis, "fetch")
    .mockImplementationOnce(
      () =>
        new Promise<Response>((resolve) => {
          finish = resolve;
        }),
    )
    .mockResolvedValueOnce(response("New owner result"));
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const { rerender } = render(
    <QueryClientProvider client={client}>
      <Host />
    </QueryClientProvider>,
  );
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
  const oldSignal = fetch.mock.calls[0]![1]!.signal!;
  identity.viewer = "viewer-2";
  rerender(
    <QueryClientProvider client={client}>
      <Host />
    </QueryClientProvider>,
  );
  await waitFor(() =>
    expect(screen.getByText("New owner result")).toBeDefined(),
  );
  expect(oldSignal.aborted).toBe(true);
  const cancel = rs.fn();
  await act(async () => {
    finish(new Response(new ReadableStream({ cancel })));
    await Promise.resolve();
  });
  await waitFor(() => expect(cancel).toHaveBeenCalledTimes(1));
  expect(screen.getByText("New owner result")).toBeDefined();
});

test("a run settling refreshes the active view revision and an ambiguous set restores ordinary source", async () => {
  const fetch = rs
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(response("First result"))
    .mockResolvedValueOnce(response("Updated result"))
    .mockImplementation(async () => response("Ambiguous result"));
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const { rerender } = render(
    <QueryClientProvider client={client}>
      <Host settled={false} />
    </QueryClientProvider>,
  );
  await waitFor(() => expect(screen.getByText("First result")).toBeDefined());
  rerender(
    <QueryClientProvider client={client}>
      <Host />
    </QueryClientProvider>,
  );
  await waitFor(() => expect(screen.getByText("Updated result")).toBeDefined());
  rerender(
    <QueryClientProvider client={client}>
      <Host
        paths={[...presented, "/mnt/user-data/outputs/results/other.view.json"]}
      />
    </QueryClientProvider>,
  );
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(4));
  expect(screen.getByText("Ordinary source")).toBeDefined();
});
