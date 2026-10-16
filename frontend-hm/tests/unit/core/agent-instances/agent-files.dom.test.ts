import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";

rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "/backend" }));
import {
  changeAgentFile,
  putAgentFile,
  uploadConversationFile,
} from "@/core/agent-instances/agent-files";
import { InstanceApiError } from "@/core/agent-instances/api";

const instance = "a".repeat(32);
const operation = "b".repeat(32);
class Transfer {
  static all: Transfer[] = [];
  method = "";
  url = "";
  body: unknown;
  headers: Record<string, string> = {};
  timeout = 0;
  withCredentials = false;
  status = 201;
  responseText = "{}";
  upload = {
    onprogress: null as
      | ((event: {
          lengthComputable: boolean;
          loaded: number;
          total: number;
        }) => void)
      | null,
  };
  onload: (() => void) | null = null;
  onerror: (() => void) | null = null;
  ontimeout: (() => void) | null = null;
  onabort: (() => void) | null = null;
  abort = rs.fn(() => this.onabort?.());
  constructor() {
    Transfer.all.push(this);
  }
  open(method: string, url: string) {
    this.method = method;
    this.url = url;
  }
  setRequestHeader(key: string, value: string) {
    this.headers[key] = value;
  }
  send(body: unknown) {
    this.body = body;
  }
  finish(status: number, body: unknown) {
    this.status = status;
    this.responseText = JSON.stringify(body);
    this.onload?.();
  }
}
beforeEach(() => {
  Transfer.all = [];
  rs.stubGlobal("XMLHttpRequest", Transfer);
  document.cookie = "csrf_token=current-cookie; path=/";
});
afterEach(() => {
  rs.unstubAllGlobals();
  document.cookie = "csrf_token=; Max-Age=0; path=/";
});

test("attaches one multipart file directly, preserving progress and the caller's operation", async () => {
  const progress = rs.fn();
  const file = new File([new Uint8Array(2 * 1024 * 1024)], "input.txt");
  const pending = uploadConversationFile(
    instance,
    "thread/with space",
    file,
    operation,
    { onProgress: progress },
  );
  const xhr = Transfer.all[0]!;
  expect(xhr.method).toBe("POST");
  expect(xhr.url).toBe(
    `/backend/api/agent-instances/${instance}/conversations/thread%2Fwith%20space/files?operation_id=${operation}`,
  );
  expect(xhr.timeout).toBe(49_000);
  expect(xhr.withCredentials).toBe(true);
  expect(xhr.headers).toEqual({ "X-CSRF-Token": "current-cookie" });
  expect(xhr.body).toBeInstanceOf(FormData);
  expect(Array.from((xhr.body as FormData).keys())).toEqual(["file"]);
  xhr.upload.onprogress?.({ lengthComputable: true, loaded: 1, total: 2 });
  expect(progress).toHaveBeenCalledWith(50);
  const saved = {
    path: "uploads/input (2).txt",
    sandbox_path: "/mnt/user-data/uploads/input (2).txt",
    size: file.size,
    sha256: "c".repeat(64),
    deduplicated: false,
  };
  xhr.finish(201, saved);
  await expect(pending).resolves.toEqual(saved);
});

test("saves raw content with the expected hash and no multipart wrapper", async () => {
  const bytes = new Blob(["hello"]);
  const sha = "d".repeat(64);
  const pending = putAgentFile(instance, "outputs/a #.txt", bytes, operation, {
    expectedSha256: sha,
  });
  const xhr = Transfer.all[0]!;
  const query = new URL(xhr.url, "http://localhost").searchParams;
  expect(xhr.method).toBe("PUT");
  expect(xhr.body).toBe(bytes);
  expect(query.get("path")).toBe("outputs/a #.txt");
  expect(query.get("operation_id")).toBe(operation);
  expect(query.get("expected_sha256")).toBe(sha);
  expect(xhr.timeout).toBeGreaterThanOrEqual(45_000);
  xhr.finish(200, {
    path: "outputs/a #.txt",
    size: 5,
    sha256: sha,
    deduplicated: false,
  });
  await expect(pending).resolves.toMatchObject({ size: 5 });
});

test("sends exactly the strict file action body and uses the longer deadline", async () => {
  const body = {
    operation_id: operation,
    action: "rename" as const,
    path: "old.txt",
    destination: "new.txt",
  };
  const pending = changeAgentFile(instance, body);
  const xhr = Transfer.all[0]!;
  expect(xhr.url).toBe(`/backend/api/agent-instances/${instance}/home/files`);
  expect(JSON.parse(xhr.body as string)).toEqual(body);
  expect(xhr.headers["Content-Type"]).toBe("application/json");
  expect(xhr.timeout).toBeGreaterThanOrEqual(45_000);
  xhr.finish(200, { complete: true, path: "new.txt" });
  await expect(pending).resolves.toEqual({ complete: true, path: "new.txt" });
});

test.each([409, 413, 503, 507])(
  "retains structured metadata and never retries HTTP %s",
  async (status) => {
    const pending = putAgentFile(
      instance,
      "input.txt",
      new Blob(["x"]),
      operation,
    );
    const failed = pending.catch((error: unknown) => error);
    Transfer.all[0]!.finish(status, {
      detail: "diagnostic",
      reason: "storage-busy",
      execution_reason: "operator-hold",
      cause: "workflow",
    });
    const error = await failed;
    expect(error).toBeInstanceOf(InstanceApiError);
    expect(error).toMatchObject({
      status,
      reason: "storage-busy",
      executionReason: "operator-hold",
      extra: { detail: "diagnostic", cause: "workflow" },
    });
    expect(Transfer.all).toHaveLength(1);
  },
);

test.each(["onerror", "ontimeout"] as const)(
  "settles %s without retrying",
  async (event) => {
    const pending = putAgentFile(instance, "a", new Blob(["x"]), operation);
    const failed = expect(pending).rejects.toThrow();
    Transfer.all[0]![event]?.();
    await failed;
    expect(Transfer.all).toHaveLength(1);
  },
);

test("aborts a retired transfer and ignores a late response", async () => {
  const controller = new AbortController();
  const pending = putAgentFile(instance, "a", new Blob(["x"]), operation, {
    signal: controller.signal,
  });
  const failed = expect(pending).rejects.toMatchObject({ name: "AbortError" });
  const xhr = Transfer.all[0]!;
  const late = xhr.onload;
  controller.abort();
  await failed;
  expect(xhr.abort).toHaveBeenCalledTimes(1);
  xhr.status = 401;
  late?.();
  expect(Transfer.all).toHaveLength(1);
});

test("does not send after the owner has already retired", async () => {
  const controller = new AbortController();
  controller.abort();
  await expect(
    putAgentFile(instance, "a", new Blob(["x"]), operation, {
      signal: controller.signal,
    }),
  ).rejects.toMatchObject({ name: "AbortError" });
  expect(Transfer.all).toHaveLength(0);
});
