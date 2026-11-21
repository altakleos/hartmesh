import { afterEach, beforeEach, expect, rs, test } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, waitFor } from "@testing-library/react";

const fixture = rs.hoisted(() => ({
  account: "a",
  permissions: ["agents:read", "agents:write"],
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: { id: fixture.account, permissions: fixture.permissions },
  }),
}));
rs.mock("@/core/agent-instances/agent-files", () => ({
  putAgentFile: rs.fn(),
}));
rs.mock("@/core/agent-instances/api", () => ({
  ...rs.requireActual<typeof instanceApi>("@/core/agent-instances/api"),
  getInstance: rs.fn(),
}));
rs.mock("@/core/spaces/api", () => ({
  ...rs.requireActual<typeof spaceApi>("@/core/spaces/api"),
  getSpace: rs.fn(),
}));
rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));

import { putAgentFile } from "@/core/agent-instances/agent-files";
import type * as instanceApi from "@/core/agent-instances/api";
import type { AgentInstanceDetail } from "@/core/agent-instances/api";
import { getInstance } from "@/core/agent-instances/api";
import {
  HomeFileTransferProvider,
  useHomeFileTransfers,
} from "@/core/agent-instances/home-transfers";
import { fetch as fetcher } from "@/core/api/fetcher";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import type * as spaceApi from "@/core/spaces/api";
import type { StorageSpace } from "@/core/spaces/api";
import { getSpace } from "@/core/spaces/api";

const instance = {
  id: "a".repeat(32),
  home_id: "home",
  home_generation: 2,
  generation: 1,
  permissions: 3,
  status: "active",
} as AgentInstanceDetail;
const home = {
  id: "home",
  generation: 2,
  permissions: 3,
  status: "active",
} as StorageSpace;
const saved = {
  path: "uploads/note (2).txt",
  size: 1,
  sha256: "1".repeat(64),
  deduplicated: false,
};
let transfers: ReturnType<typeof useHomeFileTransfers>;
const clients: QueryClient[] = [];
let resourcePermissions = 3;
function Tab({ folder }: { folder: string }) {
  transfers = useHomeFileTransfers({
    instance,
    homeGeneration: 2,
    homePermissions: resourcePermissions,
    folder,
    enabled: true,
  });
  return (
    <div>
      {transfers.rows.map((r) => r.result?.path ?? r.file.name).join(",")}
    </div>
  );
}
function show() {
  const client = new QueryClient();
  clients.push(client);
  function tree(folder?: string) {
    return (
      <QueryClientProvider client={client}>
        <FileActionLifetimeProvider>
          <HomeFileTransferProvider>
            {folder && <Tab folder={folder} />}
          </HomeFileTransferProvider>
        </FileActionLifetimeProvider>
      </QueryClientProvider>
    );
  }
  const view = render(tree("uploads"));
  return { ...view, go: (folder?: string) => view.rerender(tree(folder)) };
}
function pendingPut() {
  let finish!: () => void;
  rs.mocked(putAgentFile).mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finish = () => resolve(saved);
      }),
  );
  void act(() => transfers.add([new File(["x"], "note.txt")]));
  return () => finish();
}
beforeEach(() => {
  resourcePermissions = 3;
  fixture.account = "a";
  fixture.permissions = ["agents:read", "agents:write"];
  rs.stubGlobal(
    "fetch",
    rs.fn(async () =>
      Response.json({
        id: fixture.account,
        email: "reader@example.test",
        system_role: "user",
        permissions: fixture.permissions,
      }),
    ),
  );
  rs.mocked(getInstance).mockReset().mockResolvedValue(instance);
  rs.mocked(getSpace).mockReset().mockResolvedValue(home);
  rs.mocked(putAgentFile).mockReset();
  rs.mocked(fetcher).mockReset();
});
afterEach(() => {
  cleanup();
  rs.unstubAllGlobals();
  clients.splice(0).forEach((c) => c.clear());
});

test("a Home upload keeps its original destination and actual saved name after both Files views unmount", async () => {
  const view = show();
  const finish = pendingPut();
  await waitFor(() => expect(putAgentFile).toHaveBeenCalledOnce());
  const call = rs.mocked(putAgentFile).mock.calls[0]!;
  expect(call[1]).toBe("uploads/note.txt");
  view.go();
  expect(call[4]?.signal?.aborted).toBe(false);
  await act(async () => finish());
  view.go("outputs");
  await waitFor(() => expect(transfers.rows[0]?.result?.path).toBe(saved.path));
  expect(transfers.rows[0]?.folder).toBe("uploads");
  expect(putAgentFile).toHaveBeenCalledOnce();
});
test.each(["account", "ceiling", "home", "inspect", "write"] as const)(
  "a %s change retires the detached upload before its result can appear",
  async (change) => {
    const view = show();
    const finish = pendingPut();
    await waitFor(() => expect(putAgentFile).toHaveBeenCalledOnce());
    const signal = rs.mocked(putAgentFile).mock.calls[0]![4]?.signal;
    view.go();
    if (change === "home")
      rs.mocked(getSpace).mockResolvedValue({ ...home, generation: 3 });
    if (change === "inspect")
      rs.mocked(getInstance).mockResolvedValue({ ...instance, permissions: 1 });
    if (change === "write")
      rs.mocked(getSpace).mockResolvedValue({ ...home, permissions: 1 });
    if (change === "ceiling") fixture.permissions = ["agents:read"];
    if (change === "account") {
      fixture.account = "b";
      view.go();
    }
    void act(() => window.dispatchEvent(new Event("focus")));
    await waitFor(() => expect(signal?.aborted).toBe(true));
    await act(async () => finish());
    view.go("uploads");
    expect(transfers.rows).toEqual([]);
    expect(putAgentFile).toHaveBeenCalledOnce();
  },
);
test("a lost response settles the original operation by GET after switching routes", async () => {
  const view = show();
  let interrupt!: () => void;
  rs.mocked(putAgentFile).mockImplementationOnce(
    () =>
      new Promise((_, reject) => {
        interrupt = () => reject(new TypeError("Failed to fetch"));
      }),
  );
  rs.mocked(fetcher).mockResolvedValue(
    new Response(JSON.stringify({ status: "complete", file: saved })),
  );
  void act(() => transfers.add([new File(["x"], "note.txt")]));
  await waitFor(() => expect(putAgentFile).toHaveBeenCalledOnce());
  const id = rs.mocked(putAgentFile).mock.calls[0]![3];
  view.go();
  await act(async () => interrupt());
  await waitFor(() => expect(fetcher).toHaveBeenCalled());
  view.go("uploads");
  await waitFor(() => expect(transfers.rows[0]?.result?.path).toBe(saved.path));
  expect(rs.mocked(fetcher).mock.calls[0]![0]).toContain(
    "/home/operations/" + id,
  );
  expect(putAgentFile).toHaveBeenCalledOnce();
});

test("local refusals have bounded history and make no detached grant probes", async () => {
  const view = show();
  const invalid = new File(["x"], "invalid/name");
  void act(() => transfers.add(Array.from({ length: 110 }, () => invalid)));
  expect(transfers.rows.length).toBeLessThanOrEqual(100);
  view.go();
  void act(() => window.dispatchEvent(new Event("focus")));
  expect(getInstance).not.toHaveBeenCalled();
  expect(putAgentFile).not.toHaveBeenCalled();
});
test("a confirmed mounted grant loss immediately hides and retires saved filenames", async () => {
  const view = show();
  rs.mocked(putAgentFile).mockResolvedValue(saved);
  void act(() => transfers.add([new File(["x"], "note.txt")]));
  await waitFor(() => expect(transfers.rows[0]?.status).toBe("saved"));
  resourcePermissions = 1;
  view.go("uploads");
  expect(transfers.rows).toEqual([]);
  resourcePermissions = 3;
  view.go("uploads");
  expect(transfers.rows).toEqual([]);
});
