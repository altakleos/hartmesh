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
  attachments: [] as { id: string; phase: string }[],
  operations: [] as {
    operation_id: string;
    phase: string;
    request: { action: string };
  }[],
}));
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

function show() {
  return render(
    <I18nProvider initialLocale="en-US">
      <FileActionLifetimeProvider>
        <SpaceRecoveryPanel space={resource} refresh={refresh} />
      </FileActionLifetimeProvider>
    </I18nProvider>,
  );
}

beforeEach(() => {
  rs.clearAllMocks();
  state.attachments = [];
  state.operations = [];
  state.lifecycle.mockResolvedValue({});
  state.refetch.mockResolvedValue({});
});
afterEach(cleanup);

it("requires review before restoring and uses the current resource generation", async () => {
  show();
  fireEvent.click(screen.getByRole("button", { name: "Backups and recovery" }));
  fireEvent.click(screen.getByRole("button", { name: "Restore files" }));
  const dialog = screen.getByRole("dialog");
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
  const signal = state.lifecycle.mock.calls[0]?.[4] as AbortSignal;
  view.unmount();
  expect(signal.aborted).toBe(true);
  await act(async () => finish({}));
  expect(refresh).not.toHaveBeenCalled();
  expect(state.refetch).not.toHaveBeenCalled();
});
