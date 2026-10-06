import { expect, rs, test } from "@rstest/core";

import { mountSurface } from "@/core/extensions/surfaces";

test("an invalid asynchronous mount rejection is observed after immediate retirement", async () => {
  const error = rs.fn();
  const container = document.createElement("div");
  mountSurface(
    container,
    {
      id: "rejected",
      slot: "page",
      title: "Rejected",
      mount: () =>
        Promise.reject(new Error("Late rejected mount")) as unknown as {
          dispose: () => void;
        },
    },
    {
      namespace: "example.rejected",
      locale: "en-US",
      settings: {},
      callBackend: rs.fn(),
    },
    error,
  );
  await Promise.resolve();
  await Promise.resolve();
  expect(error).toHaveBeenCalledTimes(1);
  expect(container.shadowRoot?.textContent).toBe("");
});

test("an invalid asynchronous mount retires immediately and disposes its eventual controller", async () => {
  let finish!: (controller: { dispose: () => void }) => void;
  const pending = new Promise<{ dispose: () => void }>((resolve) => {
    finish = resolve;
  });
  const dispose = rs.fn();
  const error = rs.fn();
  const container = document.createElement("div");
  let signal!: AbortSignal;
  const cleanup = mountSurface(
    container,
    {
      id: "late",
      title: "Late",
      slot: "page",
      mount(root, context) {
        signal = context.signal;
        root.textContent = "partial";
        return pending as unknown as { dispose: () => void };
      },
    },
    {
      namespace: "example.late",
      locale: "en-US",
      settings: {},
      callBackend: rs.fn(),
    },
    error,
  );
  expect(signal.aborted).toBe(true);
  expect(error).toHaveBeenCalledTimes(1);
  expect(container.shadowRoot?.textContent).toBe("");
  finish({ dispose });
  await pending;
  await Promise.resolve();
  cleanup();
  expect(dispose).toHaveBeenCalledTimes(1);
});

test("surface cleanup aborts outstanding work and rejects late backend calls", async () => {
  const container = document.createElement("div");
  const dispose = rs.fn();
  const backend = rs.fn(async () => ({}));
  let callLater: (
    name: string,
    payload: Record<string, unknown>,
  ) => Promise<unknown> = rs.fn();
  let signal: AbortSignal | undefined;
  const cleanup = mountSurface(
    container,
    {
      id: "picker",
      slot: "page",
      title: "Picker",
      mount(root, context) {
        signal = context.signal;
        callLater = context.callBackend;
        root.textContent = "PLUGIN UI";
        return { dispose };
      },
    },
    {
      namespace: "community.example",
      locale: "en",
      settings: {},
      threadId: "a",
      callBackend: backend,
    },
    rs.fn(),
  );
  expect(container.shadowRoot?.textContent).toBe("PLUGIN UI");
  await callLater("search", {});
  cleanup();
  cleanup();
  await expect(callLater("search", {})).rejects.toThrow();
  expect(backend).toHaveBeenCalledTimes(1);
  expect(signal?.aborted).toBe(true);
  expect(dispose).toHaveBeenCalledTimes(1);
  expect(container.shadowRoot?.textContent).toBe("");
});

test("a failed mount is isolated and cannot leave its partial UI behind", () => {
  const error = rs.fn();
  const container = document.createElement("div");
  mountSurface(
    container,
    {
      id: "bad",
      slot: "page",
      title: "Broken",
      mount(root) {
        root.textContent = "partial";
        throw new Error("broken");
      },
    },
    {
      namespace: "community.example",
      locale: "en",
      settings: {},
      callBackend: rs.fn(),
    },
    error,
  );
  expect(error).toHaveBeenCalledTimes(1);
  expect(container.shadowRoot?.textContent).toBe("");
});

for (const failMount of [false, true]) {
  test(`surface ${failMount ? "mount failure" : "cleanup"} aborts navigation and rejects stale callbacks`, async () => {
    let openLater!: (id: string) => Promise<void>;
    let pending!: Promise<void>;
    let finish!: () => void;
    const navigate = rs.fn();
    const open = rs.fn(async (_id: string, signal: AbortSignal) => {
      await new Promise<void>((resolve) => {
        finish = resolve;
      });
      signal.throwIfAborted();
      navigate();
    });
    const cleanup = mountSurface(
      document.createElement("div"),
      {
        id: "library",
        slot: "page",
        title: "Library",
        mount(_root, context) {
          openLater = context.openConversation!;
          pending = openLater("thread");
          if (failMount) throw new Error("mount failed");
          return { dispose: rs.fn() };
        },
      },
      {
        namespace: "bookmarks",
        locale: "en",
        settings: {},
        callBackend: rs.fn(),
        openConversation: open,
      },
      rs.fn(),
    );
    if (!failMount) cleanup();
    finish();
    await expect(pending).rejects.toThrow();
    await expect(openLater("thread")).rejects.toThrow();
    expect(open).toHaveBeenCalledTimes(1);
    expect(navigate).not.toHaveBeenCalled();
  });
}
