import { afterEach, expect, rs, test } from "@rstest/core";
import { act, cleanup, render, waitFor } from "@testing-library/react";

import { PluginSurfaces } from "@/components/workspace/plugin-surfaces";

const host = rs.hoisted(() => ({
  getSpace: rs.fn(),
  mount: rs.fn(() => ({ dispose: rs.fn() })),
  user: "alice",
}));
rs.mock("next/navigation", () => ({ useRouter: () => ({ push: rs.fn() }) }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: host.user } }),
}));
rs.mock("@/core/spaces/api", () => ({ getSpace: host.getSpace }));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({
    locale: "en",
    t: { extensions: { viewFailed: "Unavailable" } },
  }),
}));
rs.mock("@/core/extensions/hooks", () => ({
  useFrontendServices: () => ({
    conversationText: rs.fn(),
    showMessage: rs.fn(),
  }),
  useFrontendExtensions: () => ({
    data: [
      {
        namespace: "storage.wiki",
        module: "wiki.v1",
        entry: "installed",
        title: "Wiki",
        description: "",
        settings: { enabled: true },
        storage_api_version: 1,
        storage_capabilities: { available: true },
        extension: {
          apiVersion: 1,
          resourceApiVersion: 1,
          module: "wiki.v1",
          surfaces: [
            { id: "wiki", title: "Wiki", slot: "page", mount: host.mount },
          ],
        },
      },
    ],
  }),
}));

afterEach(() => {
  cleanup();
  rs.clearAllMocks();
  host.user = "alice";
});

test("resource page mounts only after the authenticated host resolves current access", async () => {
  const id = "a".repeat(32);
  host.getSpace.mockResolvedValue({
    id,
    name: "Wiki",
    mode: "native",
    generation: 4,
    permissions: 1,
    backing_handle: "private-provider-handle",
  });
  render(<PluginSurfaces slot="page" resourceId={id} />);
  await waitFor(() => expect(host.mount).toHaveBeenCalledTimes(1));
  const [, context] = host.mount.mock.calls[0]! as unknown as [
    HTMLElement,
    { resource: object },
  ];
  expect(context.resource).toEqual({
    id,
    name: "Wiki",
    mode: "native",
    generation: 4,
    permissions: 1,
  });
  expect(Object.isFrozen(context.resource)).toBe(true);
});

test("denied resource access never reaches the installed surface", async () => {
  host.getSpace.mockRejectedValue(new Error("404"));
  const view = render(
    <PluginSurfaces slot="page" resourceId={"a".repeat(32)} />,
  );
  await waitFor(() =>
    expect(view.getByRole("alert").textContent).toBe("Unavailable"),
  );
  expect(host.mount).not.toHaveBeenCalled();
});

test("retiring a resource page cancels its metadata read and rejects late mounts", async () => {
  let finish!: (resource: object) => void;
  let signal!: AbortSignal;
  host.getSpace.mockImplementation((_id: string, current: AbortSignal) => {
    signal = current;
    return new Promise((resolve) => {
      finish = resolve;
    });
  });
  const id = "a".repeat(32);
  const view = render(<PluginSurfaces slot="page" resourceId={id} />);
  view.unmount();
  expect(signal.aborted).toBe(true);
  await act(async () => {
    finish({ id, name: "Old", mode: "native", generation: 1, permissions: 1 });
    await Promise.resolve();
  });
  expect(host.mount).not.toHaveBeenCalled();
});
