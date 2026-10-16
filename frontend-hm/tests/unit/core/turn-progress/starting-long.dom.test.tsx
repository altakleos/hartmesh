import { afterEach, expect, test, rs } from "@rstest/core";
import { act, cleanup, renderHook } from "@testing-library/react";

import { useWorkspaceStartingLong } from "@/core/turn-progress/starting-long";
import type { TurnProgress } from "@/core/turn-progress/types";

afterEach(() => {
  cleanup();
  rs.useRealTimers();
});
test("long startup is based on receipt time and retires on the next run", async () => {
  rs.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  const progress: TurnProgress = {
    runId: "run",
    stage: "workspace_starting",
    atMs: 900_000,
    receivedAt: Date.now() - 5_000,
  };
  const view = renderHook(
    ({ progress }: { progress: TurnProgress | null }) =>
      useWorkspaceStartingLong(progress),
    { initialProps: { progress } },
  );
  expect(view.result.current).toBe(false);
  await act(async () => rs.advanceTimersByTimeAsync(24_999));
  expect(view.result.current).toBe(false);
  await act(async () => rs.advanceTimersByTimeAsync(1));
  expect(view.result.current).toBe(true);
  view.rerender({
    progress: { ...progress, runId: "next", receivedAt: Date.now() },
  });
  expect(view.result.current).toBe(false);
  view.unmount();
  await act(async () => rs.advanceTimersByTimeAsync(30_000));
});
