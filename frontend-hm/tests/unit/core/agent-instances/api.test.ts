import { beforeEach, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "/backend" }));
import {
  changeInstanceLifecycle,
  createInstanceConversation,
  getConversationInstance,
} from "@/core/agent-instances/api";
import * as api from "@/core/agent-instances/api";
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

it("reads deployment-defined roles without a hard-coded business catalogue", async () => {
  const roles = [
    {
      id: "research",
      name: "Research",
      description: "Explore a topic",
      soul: "Follow the brief",
      skills: ["search"],
      work_policy: null,
    },
  ];
  mocked.mockResolvedValueOnce(Response.json({ roles }));
  expect(await api.listInstanceRoles()).toEqual({ roles });
  expect(mocked.mock.calls[0]![0]).toBe("/backend/api/agent-instances/roles");
});

it("pages conversations with server-owned requester identities and activity", async () => {
  const conversations = [
    {
      thread_id: "thread",
      requester_id: "reader",
      title: null,
      updated_at: "2026-10-16T00:00:00Z",
      running: true,
    },
  ];
  mocked.mockResolvedValueOnce(Response.json({ conversations }));
  expect(await api.listInstanceConversations(INSTANCE, 50)).toEqual({
    conversations,
  });
  expect(mocked.mock.calls[0]![0]).toBe(
    `/backend/api/agent-instances/${INSTANCE}/conversations?limit=50&offset=50`,
  );
});

it("stops the exact Work attempt through its command endpoint", async () => {
  const body: api.WorkStopCommand = {
    operation_id: "c".repeat(32),
    expected_revision: 7,
    expected_assignment_revision: 2,
    action: "stop",
    attempt_id: "d".repeat(32),
  };
  mocked.mockResolvedValueOnce(Response.json({ id: "e".repeat(32) }));
  await api.commandWork(INSTANCE, "e".repeat(32), body);
  expect(mocked.mock.calls[0]![0]).toBe(
    `/backend/api/agent-instances/${INSTANCE}/work/${"e".repeat(32)}/commands`,
  );
  expect(JSON.parse(mocked.mock.calls[0]![1]!.body as string)).toEqual(body);
});

it("retains structured refusal metadata and every other body field", async () => {
  const payload = {
    detail: "The deployment is paused",
    reason: "execution-unavailable",
    execution_reason: "operator-hold",
    per_user: 1,
    mine: 1,
    allowed: 10,
    in_use: 4,
    cause: "workflow",
  };
  mocked.mockResolvedValueOnce(Response.json(payload, { status: 409 }));
  const failure = await api
    .getInstance(INSTANCE)
    .catch((error: unknown) => error);
  expect(failure).toBeInstanceOf(api.InstanceApiError);
  expect(failure).toMatchObject({
    status: 409,
    message: payload.detail,
    reason: "execution-unavailable",
    executionReason: "operator-hold",
    extra: {
      detail: payload.detail,
      per_user: 1,
      mine: 1,
      allowed: 10,
      in_use: 4,
      cause: "workflow",
    },
  });
});

it.each(["personal", "company"] as const)(
  "lists one custody without requesting deleted or provisioning rows: %s",
  async (custody) => {
    const payload = {
      instances: [],
      ai_assistants: { per_user: 1, allowed: 10, in_use: 4, mine: 1 },
      unfinished: [
        {
          id: INSTANCE,
          name: "Researcher",
          custody,
          created_at: "2026-10-16T00:00:00Z",
        },
      ],
    };
    mocked.mockResolvedValueOnce(Response.json(payload));
    expect(await api.listInstances(custody, 7)).toEqual(payload);
    const url = new URL(
      mocked.mock.calls[0]![0] as string,
      "https://test.invalid",
    );
    expect(url.pathname).toBe("/backend/api/agent-instances");
    expect(url.searchParams.get("custody")).toBe(custody);
    expect(url.searchParams.get("offset")).toBe("7");
    expect(url.searchParams.get("limit")).toBe("100");
    expect(url.searchParams.has("include_deleted")).toBe(false);
    expect(url.searchParams.has("include_provisioning")).toBe(false);
    expect(mocked).toHaveBeenCalledTimes(1);
  },
);

it("removes an unfinished creation without reading the empty204 body", async () => {
  const response = new Response(null, { status: 204 });
  const json = rs.spyOn(response, "json");
  mocked.mockResolvedValueOnce(response);
  expect(await api.removeUnfinished(INSTANCE)).toBeUndefined();
  expect(mocked.mock.calls[0]![0]).toBe(
    `/backend/api/agent-instances/${INSTANCE}`,
  );
  expect(mocked.mock.calls[0]![1]!.method).toBe("DELETE");
  expect(json).not.toHaveBeenCalled();
  expect(mocked).toHaveBeenCalledTimes(1);
});
