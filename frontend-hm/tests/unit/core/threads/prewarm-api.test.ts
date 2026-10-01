import { beforeEach, expect, rs, test } from "@rstest/core";

const fetchWithAuth = rs.fn();

rs.mock("@/core/api/fetcher", () => ({
  fetch: fetchWithAuth,
}));

beforeEach(() => {
  fetchWithAuth.mockReset();
});

test("prewarmThreadWorkspace posts to the thread's workspace prewarm route", async () => {
  fetchWithAuth.mockResolvedValue({ ok: true, status: 202 });

  const { prewarmThreadWorkspace } = await import("@/core/threads/api");

  await expect(prewarmThreadWorkspace("thread-1")).resolves.toBeUndefined();

  expect(fetchWithAuth).toHaveBeenCalledWith(
    expect.stringContaining("/api/threads/thread-1/workspace/prewarm"),
    { method: "POST" },
  );
});

test("prewarmThreadWorkspace never rejects: a network failure is swallowed", async () => {
  fetchWithAuth.mockRejectedValue(new Error("offline"));

  const { prewarmThreadWorkspace } = await import("@/core/threads/api");

  await expect(prewarmThreadWorkspace("thread-1")).resolves.toBeUndefined();
});
