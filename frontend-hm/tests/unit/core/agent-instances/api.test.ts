import { beforeEach, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "/backend" }));
import {
  changeInstanceLifecycle,
  createInstanceConversation,
  getConversationInstance,
} from "@/core/agent-instances/api";
import { fetch } from "@/core/api/fetcher";
const mocked = rs.mocked(fetch);
const INSTANCE = "a".repeat(32);
beforeEach(() => {
  mocked.mockReset();
});
it("starts a server-minted bound conversation without claimed owner or thread", async () => {
  mocked.mockResolvedValueOnce(
    new Response(JSON.stringify({ thread_id: "minted" }), { status: 201 }),
  );
  await createInstanceConversation(INSTANCE, "b".repeat(32));
  expect(mocked.mock.calls[0]![0]).toBe(
    `/backend/api/agent-instances/${INSTANCE}/conversations`,
  );
  expect(JSON.parse(mocked.mock.calls[0]![1]!.body as string)).toEqual({
    creation_id: "b".repeat(32),
  });
});
it("keeps the same captured lifecycle request and treats202 as pending", async () => {
  mocked.mockResolvedValueOnce(
    new Response(
      JSON.stringify({ complete: false, operation_id: "c".repeat(32) }),
      { status: 202 },
    ),
  );
  const body = {
    generation: 7,
    operation_id: "c".repeat(32),
    action: "suspend" as const,
  };
  expect((await changeInstanceLifecycle(INSTANCE, body)).complete).toBe(false);
  expect(JSON.parse(mocked.mock.calls[0]![1]!.body as string)).toEqual(body);
  expect(mocked).toHaveBeenCalledTimes(1);
});
it("resolves conversation binding from its canonical endpoint", async () => {
  mocked.mockResolvedValueOnce(
    new Response(JSON.stringify({ instance: null }), { status: 200 }),
  );
  expect(await getConversationInstance("thread-1")).toEqual({ instance: null });
  expect(mocked.mock.calls[0]![0]).toBe(
    "/backend/api/agent-instances/conversations/thread-1/instance",
  );
});
it("does not return late identity data when an account retires", async () => {
  let resolve!: (response: Response) => void;
  mocked.mockImplementationOnce(
    () =>
      new Promise<Response>((r) => {
        resolve = r;
      }),
  );
  const controller = new AbortController();
  const reading = getConversationInstance("thread-1", controller.signal);
  const rejected = expect(reading).rejects.toThrow();
  controller.abort();
  resolve(
    new Response(JSON.stringify({ instance: { name: "Old account" } }), {
      status: 200,
    }),
  );
  await rejected;
});
