import { afterEach, expect, it, rs } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

rs.mock("@/core/sidecar/api", () => ({
  findLatestSidecarThread: rs.fn().mockResolvedValue(null),
}));
import {
  SidecarProvider,
  useMaybeSidecar,
} from "@/components/workspace/sidecar/context";
import { findLatestSidecarThread } from "@/core/sidecar/api";
afterEach(cleanup);
function Probe() {
  const sidecar = useMaybeSidecar();
  return (
    <>
      <button onClick={() => sidecar?.openSidecar()}>Reference action</button>
      <input aria-label="draft" defaultValue="" />
      <output>{sidecar?.open ? "open" : "closed"}</output>
    </>
  );
}
it("retires the provider and reference capability when a chat becomes bound", async () => {
  const ui = (enabled: boolean) => (
    <SidecarProvider
      parentThreadId="chat"
      context={{ mode: undefined }}
      enabled={enabled}
    >
      <Probe />
    </SidecarProvider>
  );
  const view = render(ui(true));
  fireEvent.click(screen.getByText("Reference action"));
  expect(screen.getByText("open")).toBeTruthy();
  fireEvent.change(screen.getByLabelText("draft"), {
    target: { value: "Unsaved draft" },
  });
  view.rerender(ui(false));
  fireEvent.click(screen.getByText("Reference action"));
  expect(screen.getByText("closed")).toBeTruthy();
  expect(screen.getByLabelText<HTMLInputElement>("draft").value).toBe(
    "Unsaved draft",
  );
  await waitFor(() => expect(findLatestSidecarThread).toHaveBeenCalledTimes(1));
  rs.mocked(findLatestSidecarThread).mockClear();
  view.rerender(ui(false));
  expect(findLatestSidecarThread).not.toHaveBeenCalled();
});
