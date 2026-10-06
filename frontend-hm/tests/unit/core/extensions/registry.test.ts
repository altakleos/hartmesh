import { afterEach, beforeEach, expect, rs, test } from "@rstest/core";

import { fetchFrontendExtensions } from "@/core/extensions/api";
import { loadFrontendExtensions } from "@/core/extensions/registry";

const config = rs.hoisted(() => ({ backend: "" }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => config.backend }));
rs.mock("@/core/static-mode", () => ({ isStaticWebsiteOnly: () => false }));
rs.mock("@/core/api/static-response", () => ({ staticApiResponse: rs.fn() }));

const entry = {
  namespace: "community.bookmarks",
  module: "bookmarks.v1",
  entry: `/api/plugins/modules/bookmarks.v1/${"a".repeat(64)}.mjs`,
  title: "Bookmarks",
  description: "",
  settings: { enabled: true },
};
const extension = { apiVersion: 1, module: entry.module };
const code = "export default {apiVersion: 1, module: 'bookmarks.v1'};";

test("artifact registration captures callable and label snapshots instead of retaining live getters", async () => {
  rs.spyOn(globalThis, "fetch").mockResolvedValue(new Response(code));
  const reads = new Map<string, number>();
  const mount = rs.fn(() => ({ dispose: rs.fn() }));
  const surface = Object.fromEntries(
    Object.entries({
      id: "summary",
      title: "Summary",
      kind: "native",
      mount,
    }).map(([name, value]) => [
      name,
      {
        get() {
          reads.set(name, (reads.get(name) ?? 0) + 1);
          if (reads.get(name)! > 1) throw new Error("Repeated live getter");
          return value;
        },
        enumerable: true,
      },
    ]),
  );
  const native = Object.defineProperties({}, surface);
  const moduleWithArtifact = {
    ...extension,
    artifactApiVersion: 1,
    artifacts: [native],
  };
  const installed = {
    ...entry,
    artifact_presentations: [
      {
        id: "summary",
        suffixes: [".summary.json"],
        source_max_bytes: 4096,
        preview_max_bytes: 1024,
        projection_marker: null,
      },
    ],
  };
  const result = await loadFrontendExtensions([installed], async () => ({
    default: moduleWithArtifact,
  }));
  expect(result[0]?.error).toBeUndefined();
  expect(result[0]?.extension?.artifacts?.[0]).toEqual({
    id: "summary",
    title: "Summary",
    kind: "native",
    mount,
  });
  expect([...reads.values()]).toEqual([1, 1, 1, 1]);
});

test("artifact capability is additive and stays outside the page-only surface array", async () => {
  rs.spyOn(globalThis, "fetch").mockImplementation(
    async () => new Response(code),
  );
  const descriptor = {
    id: "summary",
    suffixes: [".summary.json"],
    source_max_bytes: 1024 * 1024,
    preview_max_bytes: 1024 * 1024,
    projection_marker: null,
  };
  const surface = {
    id: "summary",
    title: "Summary",
    kind: "native",
    mount: () => ({ dispose: () => undefined }),
  };
  const artifactModule = {
    ...extension,
    artifactApiVersion: 1,
    artifacts: [surface],
  };
  const importer = rs.fn(async () => ({ default: artifactModule }));
  expect(
    (
      await loadFrontendExtensions(
        [{ ...entry, artifact_presentations: [descriptor] }],
        importer,
      )
    )[0]?.extension,
  ).toEqual(artifactModule);
  for (const invalid of [
    { ...artifactModule, artifactApiVersion: 2 },
    { ...artifactModule, artifacts: [surface, surface] },
    { ...artifactModule, artifacts: [{ ...surface, id: "unregistered" }] },
    { ...artifactModule, artifacts: [{ ...surface, kind: "script" }] },
    { ...artifactModule, artifacts: [{ ...surface, mount: undefined }] },
    { ...artifactModule, artifacts: [{ ...surface, id: "../summary" }] },
    { ...artifactModule, surfaces: [{ ...surface, slot: "artifact" }] },
  ]) {
    expect(
      (
        await loadFrontendExtensions(
          [{ ...entry, artifact_presentations: [descriptor] }],
          async () => ({ default: invalid }),
        )
      )[0]?.error,
    ).toBeTruthy();
  }
});

beforeEach(() => {
  config.backend = "";
  rs.spyOn(console, "warn").mockImplementation(() => undefined);
});
afterEach(() => {
  rs.restoreAllMocks();
  rs.useRealTimers();
});

test("bounds actual inline bytes when the module route ignores Range", async () => {
  const cancel = rs.fn();
  rs.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array(512 * 1024 + 1));
        },
        cancel,
      }),
    ),
  );
  const importer = rs.fn();
  const result = await loadFrontendExtensions([entry], importer);
  expect(result[0]?.error).toBeTruthy();
  expect(importer).not.toHaveBeenCalled();
  expect(cancel).toHaveBeenCalledTimes(1);
});

test("retired discovery cannot execute an inline module after a late authenticated read", async () => {
  const controller = new AbortController();
  let finish!: (response: Response) => void;
  rs.spyOn(globalThis, "fetch").mockImplementation(
    () =>
      new Promise<Response>((resolve) => {
        finish = resolve;
      }),
  );
  const importer = rs.fn();
  const pending = loadFrontendExtensions([entry], importer, undefined, {
    signal: controller.signal,
  });
  controller.abort();
  finish(new Response(code));
  await expect(pending).rejects.toThrow();
  expect(importer).not.toHaveBeenCalled();
});

test("rejects a descriptor belonging to a different viewer before importing code", async () => {
  const importer = rs.fn();
  const fetch = rs.spyOn(globalThis, "fetch");
  const loaded = await loadFrontendExtensions(
    [{ ...entry, viewer_id: "old-viewer" }],
    importer,
    undefined,
    { viewerId: "current-viewer" },
  );
  expect(loaded[0]?.error).toBeTruthy();
  expect(importer).not.toHaveBeenCalled();
  expect(fetch).not.toHaveBeenCalled();
});

test("a stalled native module import reaches a finite failure and releases its Blob URL", async () => {
  rs.useFakeTimers();
  rs.spyOn(globalThis, "fetch").mockResolvedValue(new Response(code));
  const importer = rs.fn(
    () => new Promise<{ default: unknown }>(() => undefined),
  );
  const revoke = rs.spyOn(URL, "revokeObjectURL");
  const pending = loadFrontendExtensions([entry], importer);
  await rs.advanceTimersByTimeAsync(30_000);
  expect((await pending)[0]?.error).toBeTruthy();
  expect(revoke).toHaveBeenCalledTimes(1);
});

for (const backend of [
  "",
  "https://backend.example",
  "https://backend.example/deerflow",
  "/gateway",
]) {
  test(`loads authenticated code with backend base ${backend || "same origin"}`, async () => {
    config.backend = backend;
    const request = rs
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response(code));
    const revoke = rs.spyOn(URL, "revokeObjectURL");
    const importer = rs.fn(async (url: string) => {
      expect(url.startsWith("blob:")).toBe(true);
      // Read the actual blob without using the spied HTTP fetch.
      const { resolveObjectURL } = await import("node:buffer");
      expect(await resolveObjectURL(url)?.text()).toBe(code);
      expect(revoke).not.toHaveBeenCalled();
      return { default: extension };
    });
    const result = await loadFrontendExtensions([entry], importer);
    expect(result[0]?.extension).toEqual(extension);
    expect(request).toHaveBeenCalledWith(
      `${backend}${entry.entry}`,
      expect.objectContaining({ credentials: "include", cache: "no-store" }),
    );
    expect(importer).toHaveBeenCalledTimes(1);
    expect(revoke).toHaveBeenCalledWith(importer.mock.calls[0]![0]);
  });
}

test("rejected HTTP responses never execute as modules", async () => {
  const request = rs.spyOn(globalThis, "fetch");
  const create = rs.spyOn(URL, "createObjectURL");
  const importer = rs.fn();
  for (const status of [403, 404, 500]) {
    request.mockResolvedValue(new Response("not javascript", { status }));
    const result = await loadFrontendExtensions([entry], importer);
    expect(result[0]?.error).toBeTruthy();
    expect(result[0]?.extension).toBeUndefined();
  }
  expect(create).not.toHaveBeenCalled();
  expect(importer).not.toHaveBeenCalled();
});

test("failed import releases its blob and does not prevent another plugin loading", async () => {
  rs.spyOn(globalThis, "fetch").mockImplementation(
    async () => new Response(code),
  );
  const revoke = rs.spyOn(URL, "revokeObjectURL");
  const importer = rs
    .fn()
    .mockRejectedValueOnce(new Error("module failed"))
    .mockResolvedValueOnce({ default: extension });
  const result = await loadFrontendExtensions([entry, entry], importer);
  expect(result[0]?.error).toBeTruthy();
  expect(result[1]?.extension).toEqual(extension);
  expect(revoke).toHaveBeenCalledTimes(2);
  for (const [url] of importer.mock.calls)
    expect(revoke).toHaveBeenCalledWith(url);
});

test("disabled, backend-only and invalid entries never fetch or import", async () => {
  const request = rs.spyOn(globalThis, "fetch");
  const importer = rs.fn();
  await loadFrontendExtensions(
    [
      { ...entry, settings: { enabled: false } },
      { ...entry, module: null, entry: null },
      { ...entry, entry: "https://untrusted.example/code.mjs" },
      { ...entry, entry: `${entry.entry}?redirect=elsewhere` },
    ],
    importer,
  );
  expect(request).not.toHaveBeenCalled();
  expect(importer).not.toHaveBeenCalled();
});

for (const backend of ["", "https://backend.example/prefix", "/gateway"]) {
  test(`packaged entries preserve resource URLs with base ${backend}`, async () => {
    config.backend = backend;
    const assets = {
      ...entry,
      transport: "assets-v1" as const,
      entry: `/api/plugins/${entry.namespace}/assets/${"b".repeat(64)}/static/dist/index.mjs`,
    };
    const importer = rs.fn();
    const assetImporter = rs.fn(async () => ({ default: extension }));
    const result = await loadFrontendExtensions(
      [assets],
      importer,
      assetImporter,
    );
    expect(result[0]?.extension).toEqual(extension);
    expect(assetImporter).toHaveBeenCalledWith(
      backend + assets.entry,
      expect.any(AbortSignal),
    );
    expect(importer).not.toHaveBeenCalled();
    assetImporter.mockClear();
    for (const path of [
      "../index.mjs",
      "%2e%2e/index.mjs",
      "index.mjs?x",
      "index.css",
    ]) {
      const invalid = {
        ...assets,
        entry: `/api/plugins/${entry.namespace}/assets/${"b".repeat(64)}/${path}`,
      };
      expect(
        (await loadFrontendExtensions([invalid], importer, assetImporter))[0]
          ?.error,
      ).toBeTruthy();
    }
    expect(assetImporter).not.toHaveBeenCalled();
  });
}

test("unknown transports and failing packaged plugins are isolated", async () => {
  const importer = rs.fn();
  const assetImporter = rs
    .fn()
    .mockRejectedValueOnce(new Error("missing chunk"))
    .mockResolvedValueOnce({ default: extension });
  const assets = {
    ...entry,
    transport: "assets-v1" as const,
    entry: `/api/plugins/${entry.namespace}/assets/${"b".repeat(64)}/index.mjs`,
  };
  const unknown = { ...entry, transport: "future-v9" as "assets-v1" };
  const result = await loadFrontendExtensions(
    [unknown, assets, assets],
    importer,
    assetImporter,
  );
  expect(result.map((item) => !!item.extension)).toEqual([false, false, true]);
  expect(importer).not.toHaveBeenCalled();
  expect(assetImporter).toHaveBeenCalledTimes(2);
});

test("discovery preserves transport negotiation through parsing and isolates newer transports", async () => {
  const assets = {
    ...entry,
    transport: "assets-v1",
    entry: `/api/plugins/${entry.namespace}/assets/${"b".repeat(64)}/index.mjs`,
  };
  rs.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(
      JSON.stringify([assets, { ...assets, transport: "future-v9" }]),
    ),
  );
  const discovered = await fetchFrontendExtensions();
  expect(discovered.map((item) => item.transport)).toEqual([
    "assets-v1",
    "future-v9",
  ]);
  const assetImporter = rs.fn(async () => ({ default: extension }));
  const loaded = await loadFrontendExtensions(
    discovered,
    rs.fn(),
    assetImporter,
  );
  expect(loaded[0]?.extension).toEqual(extension);
  expect(loaded[1]?.error).toBeTruthy();
  expect(assetImporter).toHaveBeenCalledTimes(1);
});
