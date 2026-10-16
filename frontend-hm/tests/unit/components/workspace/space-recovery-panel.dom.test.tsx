import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";

const state = rs.hoisted(() => ({
  lifecycle: rs.fn(),
  accept: rs.fn(),
  retire: rs.fn(),
  refetch: rs.fn(),
  detail: rs.fn(),
  discover: rs.fn(),
  generation: 4,
  owner: undefined as AgentInstance | undefined,
  attachments: [] as { id: string; phase: string }[],
  operations: [] as {
    operation_id: string;
    phase: string;
    request: { action: string };
  }[],
}));
rs.mock("@/core/agent-instances/api", () => ({ getInstance: state.detail }));
rs.mock("@/core/spaces/api", () => ({
  changeSpaceLifecycle: state.lifecycle,
  acceptSpaceCurrentState: state.accept,
  retireSpaceAttachments: state.retire,
}));
rs.mock("@/core/spaces/hooks", () => ({
  useSpaceRecovery: () => ({
    refetch: state.refetch,
    data: {
      backups: [
        {
          id: "b".repeat(32),
          generation: 1,
          size_bytes: 10240,
          consistency: "quiesced-filesystem",
        },
      ],
      operations: state.operations,
      attachments: state.attachments,
      can_fence: true,
    },
  }),
}));

import { SpaceRecoveryPanel } from "@/components/workspace/space-recovery-panel";
import type { AgentInstance } from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
import type { StorageSpace } from "@/core/spaces/api";

const refresh = rs.fn();
const resource = {
  id: "a".repeat(32),
  name: "Home",
  generation: 4,
  mode: "native",
  status: "active",
  permissions: 27,
} as StorageSpace;

function tree() {
  return (
    <I18nProvider initialLocale="en-US">
      <FileActionLifetimeProvider>
        <SpaceRecoveryPanel
          space={{ ...resource, generation: state.generation }}
          refresh={refresh}
          agent={state.owner}
          findAgent={state.discover}
        />
      </FileActionLifetimeProvider>
    </I18nProvider>
  );
}
function show() {
  return render(tree());
}

beforeEach(() => {
  rs.clearAllMocks();
  state.attachments = [];
  state.operations = [];
  state.owner = undefined;
  state.generation = 4;
  state.detail.mockResolvedValue({ activity: { running_runs: 0 } });
  state.discover.mockResolvedValue(undefined);
  state.lifecycle.mockResolvedValue({});
  state.refetch.mockResolvedValue({});
});
afterEach(cleanup);

it("retires an open Delete confirmation as soon as an agent owner is discovered", () => {
  const view = show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  fireEvent.click(screen.getByRole("button", { name: "Delete space" }));
  fireEvent.click(screen.getByRole("checkbox"));
  owned();
  view.rerender(tree());
  expect(screen.queryByRole("dialog")).toBeNull();
  expect(screen.queryByRole("button", { name: "Delete space" })).toBeNull();
  expect(state.lifecycle).not.toHaveBeenCalled();
});

it("retires Delete after an owned-space refusal rather than keeping Confirm actionable", async () => {
  state.lifecycle.mockRejectedValue(
    Object.assign(new Error("private diagnostic"), {
      status: 409,
      reason: "owned-space",
    }),
  );
  show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  fireEvent.click(screen.getByRole("button", { name: "Delete space" }));
  const dialog = screen.getByRole("dialog");
  fireEvent.click(within(dialog).getByRole("checkbox"));
  fireEvent.click(
    within(dialog).getByRole("button", { name: "Confirm action" }),
  );
  await waitFor(() => expect(state.discover).toHaveBeenCalledTimes(1));
  expect(screen.queryByRole("dialog")).toBeNull();
  expect(state.lifecycle).toHaveBeenCalledTimes(1);
});

it("drops an acknowledged confirmation when the storage generation changes", async () => {
  const view = show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  fireEvent.click(screen.getByRole("button", { name: "Restore files" }));
  const dialog = await screen.findByRole("dialog");
  fireEvent.click(within(dialog).getByRole("checkbox"));
  state.generation = 5;
  view.rerender(tree());
  expect(screen.queryByRole("dialog")).toBeNull();
  expect(state.lifecycle).not.toHaveBeenCalled();
});

it("retires a held activity read without leaving the next generation busy", async () => {
  owned();
  const view = show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  await waitFor(() => expect(state.detail).toHaveBeenCalledTimes(1));
  let finish!: (value: unknown) => void;
  state.detail.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  fireEvent.click(screen.getByRole("button", { name: "Create backup" }));
  const signal = state.detail.mock.calls[1]?.[1] as AbortSignal;
  state.generation = 5;
  view.rerender(tree());
  expect(signal.aborted).toBe(true);
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "Create backup" })
      .disabled,
  ).toBe(false);
  await act(async () => finish({ activity: { running_runs: 0 } }));
  expect(state.lifecycle).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Create backup" }));
  await waitFor(() => expect(state.lifecycle).toHaveBeenCalledTimes(1));
  expect(state.lifecycle.mock.calls[0]?.[1]).toBe(5);
});

function owned() {
  state.owner = {
    id: "d".repeat(32),
    name: "Researcher",
    home_id: resource.id,
    status: "active",
    permissions: 7,
  } as AgentInstance;
}

it("links an agent's Access and Settings instead of offering storage lifecycle controls", async () => {
  owned();
  state.attachments = [{ id: "c".repeat(32), phase: "active" }];
  show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  await waitFor(() => expect(state.detail).toHaveBeenCalledTimes(1));
  expect(screen.queryByRole("button", { name: "Archive space" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Delete space" })).toBeNull();
  expect(
    screen.queryByRole("button", { name: "Retire these environments" }),
  ).toBeNull();
  expect(
    screen.getByRole("link", { name: "Access" }).getAttribute("href"),
  ).toBe(`/workspace/instances?instance=${state.owner!.id}&tab=access`);
  expect(
    screen.getByRole("link", { name: "Settings" }).getAttribute("href"),
  ).toBe(`/workspace/instances?instance=${state.owner!.id}&tab=settings`);
  expect(
    screen.getByText(
      "A backup, a restore or a repair of the files closes Researcher's workspace. A reply or an assignment in progress stops, and Researcher cannot work until the backup, restore or repair has finished.",
    ),
  ).toBeDefined();
});

it("checks current work before a backup and asks before interrupting it", async () => {
  owned();
  show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  await waitFor(() => expect(state.detail).toHaveBeenCalledTimes(1));
  state.detail.mockResolvedValue({ activity: { running_runs: 1 } });
  fireEvent.click(screen.getByRole("button", { name: "Create backup" }));
  const dialog = await screen.findByRole("dialog");
  expect(state.lifecycle).not.toHaveBeenCalled();
  expect(
    within(dialog).getByText(
      "Researcher is working right now. That work stops.",
    ),
  ).toBeDefined();
  fireEvent.click(within(dialog).getByRole("checkbox"));
  fireEvent.click(
    within(dialog).getByRole("button", { name: "Confirm action" }),
  );
  await waitFor(() => expect(state.lifecycle).toHaveBeenCalledTimes(1));
  expect(state.lifecycle.mock.calls[0]?.[2]).toBe("backup");
  expect(state.detail.mock.calls.length).toBeGreaterThanOrEqual(3);
});

it("does not demand an interruption confirmation for a backup with no work in flight", async () => {
  owned();
  show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  await waitFor(() => expect(state.detail).toHaveBeenCalledTimes(1));
  fireEvent.click(screen.getByRole("button", { name: "Create backup" }));
  await waitFor(() => expect(state.lifecycle).toHaveBeenCalledTimes(1));
  expect(screen.queryByRole("dialog")).toBeNull();
});

it.each(["restore", "accept"])(
  "warns that current work stops before %s",
  async (action) => {
    owned();
    state.detail.mockResolvedValue({ activity: { running_runs: 1 } });
    if (action === "accept")
      state.operations = [
        {
          operation_id: "e".repeat(32),
          phase: "pending",
          request: { action: "restore" },
        },
      ];
    show();
    fireEvent.click(
      screen.getByRole("button", { name: "Backups and recovery" }),
    );
    await waitFor(() => expect(state.detail).toHaveBeenCalledTimes(1));
    fireEvent.click(
      screen.getByRole("button", {
        name:
          action === "accept" ? "Accept current file state" : "Restore files",
      }),
    );
    const dialog = await screen.findByRole("dialog");
    expect(
      within(dialog).getByText(
        "Researcher is working right now. That work stops.",
      ),
    ).toBeDefined();
  },
);

it("rechecks a confirmation and asks again when work started while it was open", async () => {
  owned();
  show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  await waitFor(() => expect(state.detail).toHaveBeenCalledTimes(1));
  fireEvent.click(screen.getByRole("button", { name: "Restore files" }));
  const dialog = await screen.findByRole("dialog");
  fireEvent.click(within(dialog).getByRole("checkbox"));
  state.detail.mockResolvedValue({ activity: { running_runs: 1 } });
  fireEvent.click(
    within(dialog).getByRole("button", { name: "Confirm action" }),
  );
  await waitFor(() =>
    expect(
      within(dialog).getByText(
        "Researcher is working right now. That work stops.",
      ),
    ).toBeDefined(),
  );
  expect(state.lifecycle).not.toHaveBeenCalled();
  expect(within(dialog).getByRole("checkbox").getAttribute("checked")).not.toBe(
    "true",
  );
});

it("redraws an owned-space refusal using localized guidance and refreshed ownership", async () => {
  state.lifecycle.mockRejectedValue(
    Object.assign(new Error("private server diagnostic"), {
      status: 409,
      reason: "owned-space",
    }),
  );
  const agent = {
    id: "d".repeat(32),
    name: "Researcher",
    home_id: resource.id,
    status: "active",
    permissions: 7,
  };
  state.discover.mockResolvedValue(agent);
  show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  fireEvent.click(screen.getByRole("button", { name: "Archive space" }));
  await waitFor(() => expect(state.discover).toHaveBeenCalledTimes(1));
  expect(screen.queryByText("private server diagnostic")).toBeNull();
  expect(screen.queryByRole("button", { name: "Archive space" })).toBeNull();
  expect(screen.getByRole("link", { name: "Access" })).toBeDefined();
});

it.each(["failed", "missing", "summary"])(
  "keeps protected controls hidden after %s owner discovery, and lets Refresh recover",
  async (mode) => {
    state.lifecycle.mockRejectedValue(
      Object.assign(new Error("private diagnostic"), {
        status: 409,
        reason: "owned-space",
      }),
    );
    if (mode === "failed")
      state.discover.mockRejectedValueOnce(new Error("offline"));
    if (mode === "missing") state.discover.mockResolvedValueOnce(undefined);
    if (mode === "summary")
      state.discover.mockResolvedValueOnce({
        id: "d".repeat(32),
        name: "Inventory only",
        status: "active",
        supervisor_name: null,
      });
    show();
    fireEvent.click(
      screen.getByRole("button", { name: "Backups and recovery" }),
    );
    fireEvent.click(screen.getByRole("button", { name: "Archive space" }));
    await waitFor(() => expect(state.discover).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole("button", { name: "Archive space" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Delete space" })).toBeNull();
    expect(screen.queryByRole("link", { name: "Access" })).toBeNull();
    expect(
      screen.getByText(
        "These files belong to an AI assistant or AI employee. Change who can use them on that AI assistant or AI employee's Access tab, and pause, retire or delete that AI assistant or AI employee on the Settings tab.",
      ),
    ).toBeDefined();
    state.discover.mockResolvedValue({
      id: "d".repeat(32),
      name: "Researcher",
      home_id: resource.id,
      status: "active",
      permissions: 7,
    });
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    expect(await screen.findByRole("link", { name: "Access" })).toBeDefined();
  },
);

it("does not publish late ownership from a retired page", async () => {
  let finish!: (value: unknown) => void;
  state.lifecycle.mockRejectedValue(
    Object.assign(new Error("private diagnostic"), {
      status: 409,
      reason: "owned-space",
    }),
  );
  state.discover.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  fireEvent.click(screen.getByRole("button", { name: "Archive space" }));
  await waitFor(() => expect(state.discover).toHaveBeenCalledTimes(1));
  view.unmount();
  await act(async () =>
    finish({
      id: "d".repeat(32),
      name: "Private old owner",
      home_id: resource.id,
      permissions: 7,
      status: "active",
    }),
  );
  expect(screen.queryByText(/Private old owner/)).toBeNull();
});

it("adds no running-work sentence when the agent is no longer readable", async () => {
  owned();
  state.detail.mockRejectedValue(
    Object.assign(new Error("unavailable"), { status: 404 }),
  );
  show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  await waitFor(() => expect(state.detail).toHaveBeenCalledTimes(1));
  fireEvent.click(screen.getByRole("button", { name: "Restore files" }));
  const dialog = await screen.findByRole("dialog");
  expect(
    within(dialog).queryByText(
      "Researcher is working right now. That work stops.",
    ),
  ).toBeNull();
});

it("requires review before restoring and uses the current resource generation", async () => {
  show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  fireEvent.click(screen.getByRole("button", { name: "Restore files" }));
  const dialog = await screen.findByRole("dialog");
  expect(
    within(dialog)
      .getByRole("button", { name: "Confirm action" })
      .hasAttribute("disabled"),
  ).toBe(true);
  expect(state.lifecycle).not.toHaveBeenCalled();
  fireEvent.click(within(dialog).getByRole("checkbox"));
  fireEvent.click(
    within(dialog).getByRole("button", { name: "Confirm action" }),
  );
  await waitFor(() => expect(state.lifecycle).toHaveBeenCalledTimes(1));
  expect(state.lifecycle.mock.calls[0]?.slice(0, 4)).toEqual([
    resource.id,
    4,
    "restore",
    "b".repeat(32),
  ]);
});

it("captures exact attachment scope so retirement retries cannot stop a replacement", async () => {
  state.attachments = [{ id: "c".repeat(32), phase: "active" }];
  show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  fireEvent.click(
    screen.getByRole("button", { name: "Retire these environments" }),
  );
  await waitFor(() => expect(state.retire).toHaveBeenCalledTimes(1));
  expect(state.retire.mock.calls[0]?.slice(0, 3)).toEqual([
    resource.id,
    4,
    ["c".repeat(32)],
  ]);
});

it("does not refresh a new page after a retired action finishes", async () => {
  let finish!: (value: unknown) => void;
  state.lifecycle.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  fireEvent.click(screen.getByRole("button", { name: "Create backup" }));
  await waitFor(() => expect(state.lifecycle).toHaveBeenCalledTimes(1));
  const signal = state.lifecycle.mock.calls[0]?.[4] as AbortSignal;
  view.unmount();
  expect(signal.aborted).toBe(true);
  await act(async () => finish({}));
  expect(refresh).not.toHaveBeenCalled();
  expect(state.refetch).not.toHaveBeenCalled();
});
