import { afterEach, expect, rs, test } from "@rstest/core";

import { readSourceViews } from "@/core/artifact-views/source-view";

rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("@/core/static-mode", () => ({ isStaticWebsiteOnly: () => false }));

const SOURCE = "/mnt/user-data/outputs/results/source.json";
const VIEW = "/mnt/user-data/outputs/results/result.view.json";
const body = JSON.stringify({
  format: "hartmesh.artifact-view",
  version: 1,
  title: "Related result",
  primary_source: { path: "source.json" },
  blocks: [],
  exports: [],
});
const options = () => ({
  filepath: SOURCE,
  presented: [SOURCE, VIEW],
  threadId: "thread-1",
  signal: new AbortController().signal,
});
afterEach(() => {
  rs.restoreAllMocks();
  rs.useRealTimers();
});

test("reads only a recorded bounded view and retains its own observed revision", async () => {
  const fetch = rs
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(
      new Response(body, { headers: { ETag: `"${"a".repeat(64)}"` } }),
    );
  const result = await readSourceViews(options());
  expect(result?.filepath).toBe(VIEW);
  expect(result?.revision).toBe("a".repeat(64));
  expect(result?.view.title).toBe("Related result");
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(new Headers(fetch.mock.calls[0]?.[1]?.headers).get("Range")).toBe(
    "bytes=0-1048575",
  );
});

test("refuses ambiguous or unverified candidates while preserving ordinary source selection", async () => {
  const other = "/mnt/user-data/outputs/results/other.view.json";
  const fetch = rs
    .spyOn(globalThis, "fetch")
    .mockImplementation(async () => new Response(body));
  expect(
    await readSourceViews({ ...options(), presented: [SOURCE, VIEW, other] }),
  ).toBeNull();
  fetch
    .mockReset()
    .mockResolvedValueOnce(new Response(body))
    .mockResolvedValueOnce(
      new Response(body.slice(0, 10), {
        status: 206,
        headers: { "Content-Range": `bytes 0-9/${body.length}` },
      }),
    );
  await expect(
    readSourceViews({ ...options(), presented: [SOURCE, VIEW, other] }),
  ).rejects.toThrow();
});

test("the candidate limit does not fetch a partial set", async () => {
  const fetch = rs
    .spyOn(globalThis, "fetch")
    .mockImplementation(async () => new Response(body));
  const views = Array.from(
    { length: 9 },
    (_, index) => `/mnt/user-data/outputs/results/${index}.view.json`,
  );
  expect(
    await readSourceViews({ ...options(), presented: [SOURCE, ...views] }),
  ).toBeNull();
  expect(fetch).not.toHaveBeenCalled();
});

test("a malformed sibling candidate does not let a valid view become a hidden winner", async () => {
  rs.spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(new Response(body))
    .mockResolvedValueOnce(new Response("{}"));
  await expect(
    readSourceViews({
      ...options(),
      presented: [
        SOURCE,
        VIEW,
        "/mnt/user-data/outputs/results/other.view.json",
      ],
    }),
  ).rejects.toThrow();
});

test("two readers are owned by the query and no queued candidate starts after retirement", async () => {
  const controller = new AbortController();
  const complete: ((response: Response) => void)[] = [];
  const fetch = rs.spyOn(globalThis, "fetch").mockImplementation(
    () =>
      new Promise<Response>((resolve) => {
        complete.push(resolve);
      }),
  );
  const views = Array.from(
    { length: 8 },
    (_, index) => `/mnt/user-data/outputs/results/${index}.view.json`,
  );
  const pending = readSourceViews({
    ...options(),
    signal: controller.signal,
    presented: [SOURCE, ...views],
  });
  expect(fetch).toHaveBeenCalledTimes(2);
  controller.abort();
  await expect(pending).rejects.toThrow();
  expect(fetch).toHaveBeenCalledTimes(2);
  for (const finish of complete) finish(new Response(body));
  await Promise.resolve();
  expect(fetch).toHaveBeenCalledTimes(2);
});

test("a stalled candidate reaches the view read deadline without waiting for its transport", async () => {
  rs.useFakeTimers();
  rs.spyOn(globalThis, "fetch").mockReturnValue(
    new Promise<Response>(() => undefined),
  );
  const pending = readSourceViews(options());
  const rejected = expect(pending).rejects.toThrow();
  await rs.advanceTimersByTimeAsync(20_001);
  await rejected;
});
