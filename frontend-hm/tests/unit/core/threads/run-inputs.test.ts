import type { Message } from "@langchain/langgraph-sdk";
import { afterEach, describe, expect, it, rs } from "@rstest/core";

import {
  changeRunInput,
  reconcileRunInputs,
  rememberUnsettledRunInputs,
  RunInputRequestError,
  selectRunToFollow,
  submitRunInput,
  type RunInput,
} from "@/core/threads/run-inputs";

const input = (overrides: Partial<RunInput> = {}): RunInput => ({
  id: "input-1",
  target_run_id: "run-1",
  message_id: "message-1",
  text: "Use the shorter outline",
  mode: "steer",
  status: "pending",
  created_at: "2026-11-04T00:00:00Z",
  ...overrides,
});

afterEach(() => {
  rs.unstubAllGlobals();
});

describe("reconcileRunInputs", () => {
  it("shows a steer input as applied once the run's state lists it", () => {
    const [item] = reconcileRunInputs(
      [input()],
      { run_id: "run-1", ids: ["input-1"] },
      [],
    );
    expect(item?.status).toBe("applied");
  });

  it("shows a steer input as applied once its message is in that run", () => {
    const message = {
      type: "human",
      id: "message-1",
      content: "Use the shorter outline",
      run_id: "run-1",
      additional_kwargs: { run_input_id: "input-1" },
    } as unknown as Message;
    expect(reconcileRunInputs([input()], null, [message])[0]?.status).toBe(
      "applied",
    );
  });

  it("leaves queue inputs and other runs to the Gateway", () => {
    const applied = { run_id: "run-1", ids: ["input-1"] };
    expect(
      reconcileRunInputs([input({ mode: "queue" })], applied, [])[0]?.status,
    ).toBe("pending");
    expect(
      reconcileRunInputs([input({ target_run_id: "run-2" })], applied, [])[0]
        ?.status,
    ).toBe("pending");
    expect(
      reconcileRunInputs([input({ status: "not_applied" })], applied, [])[0]
        ?.status,
    ).toBe("not_applied");
  });
});

describe("run-input requests", () => {
  it("reports the HTTP status of a refused submission", async () => {
    rs.stubGlobal(
      "fetch",
      rs.fn(async () =>
        Response.json(
          { detail: "This Run has not enabled interactive inputs" },
          {
            status: 409,
          },
        ),
      ),
    );
    const error = await submitRunInput({
      threadId: "thread-1",
      runId: "run-1",
      requestId: "request-1",
      text: "Continue",
      mode: "queue",
    }).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(RunInputRequestError);
    expect(error).toMatchObject({
      status: 409,
      message: "This Run has not enabled interactive inputs",
    });
  });

  it("cancels with DELETE and retries with POST on the input's own run", async () => {
    const calls: Array<[string, string | undefined]> = [];
    rs.stubGlobal(
      "fetch",
      rs.fn(async (url: string, init?: RequestInit) => {
        calls.push([url, init?.method]);
        return Response.json({});
      }),
    );
    await changeRunInput("thread-1", input(), "cancel");
    await changeRunInput("thread-1", input(), "retry");
    expect(calls[0]?.[0]).toMatch(
      /\/api\/threads\/thread-1\/runs\/run-1\/inputs\/input-1$/,
    );
    expect(calls[0]?.[1]).toBe("DELETE");
    expect(calls[1]?.[0]).toMatch(/\/inputs\/input-1\/retry$/);
    expect(calls[1]?.[1]).toBe("POST");
  });
});

describe("selectRunToFollow", () => {
  const queued = (overrides: Partial<RunInput>) =>
    input({ mode: "queue", ...overrides });

  it("follows the run taking inputs unless this tab streams it", () => {
    const state = { items: [], active_run_id: "run-2" };
    const base = { state, unsettledSeen: new Set<string>() };
    expect(
      selectRunToFollow({ ...base, streamed: new Set(), pointer: null }),
    ).toBe("run-2");
    expect(
      selectRunToFollow({ ...base, streamed: new Set(), pointer: "run-2" }),
    ).toBeNull();
    expect(
      selectRunToFollow({
        ...base,
        streamed: new Set(["run-2"]),
        pointer: null,
      }),
    ).toBeNull();
  });

  it("follows a queued input's run only when this view saw it waiting", () => {
    const seen = new Set<string>();
    rememberUnsettledRunInputs(
      { items: [queued({ status: "pending" })], active_run_id: "run-1" },
      seen,
    );
    const applied = {
      items: [queued({ status: "applied", run_id: "run-2" })],
      active_run_id: null,
    };
    expect(
      selectRunToFollow({
        state: applied,
        unsettledSeen: seen,
        streamed: new Set(["run-1"]),
        pointer: null,
      }),
    ).toBe("run-2");
    expect(
      selectRunToFollow({
        state: applied,
        unsettledSeen: new Set(),
        streamed: new Set(),
        pointer: null,
      }),
    ).toBeNull();
  });

  it("never follows a steer input's own run", () => {
    const seen = new Set(["run-1:input-1"]);
    expect(
      selectRunToFollow({
        state: {
          items: [input({ status: "applied", run_id: "run-1" })],
          active_run_id: null,
        },
        unsettledSeen: seen,
        streamed: new Set(),
        pointer: null,
      }),
    ).toBeNull();
  });
});
