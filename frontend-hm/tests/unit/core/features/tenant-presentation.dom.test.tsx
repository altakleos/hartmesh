import { afterEach, expect, rs, test } from "@rstest/core";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

const state = { user: { id: "one", system_role: "user" } };
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => state }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("@/core/static-mode", () => ({ isStaticWebsiteOnly: () => false }));
rs.mock("@/core/product/context", () => ({
  useProductIdentity: () => DEFAULT_IDENTITY,
}));

import {
  canonicalPresentation,
  parseTenantPresentation,
  presentationDigest,
  TenantPresentationProvider,
  useTenantPresentation,
} from "@/core/features/tenant-presentation";
import { DEFAULT_IDENTITY } from "@/core/product/identity";

const logo = "data:image/png;base64,iVBORw0KGgo=";
async function profile(name = "Example Tenant One") {
  const body = {
    schema_version: 1,
    mode: "tenant",
    product_sha256: await presentationDigest(DEFAULT_IDENTITY),
    tenant: { name, logo },
  };
  return { ...body, snapshot_id: await presentationDigest(body) };
}
function Probe() {
  const value = useTenantPresentation();
  return (
    <p>{value && value.mode !== "legacy" ? value.tenant?.name : value?.mode}</p>
  );
}
afterEach(() => {
  cleanup();
  rs.restoreAllMocks();
  state.user = { id: "one", system_role: "user" };
});

test("canonical JSON matches the Python ASCII contract including Unicode", () => {
  expect(canonicalPresentation({ z: "🦊 é\u007f", a: { c: 1, b: null } })).toBe(
    '{"a":{"b":null,"c":1},"z":"\\ud83e\\udd8a \\u00e9\\u007f"}',
  );
});

test("complete input binds to both content and this document's product", async () => {
  const body = await profile();
  expect((await parseTenantPresentation(body, DEFAULT_IDENTITY)).mode).toBe(
    "tenant",
  );
  await expect(
    parseTenantPresentation(
      { ...body, tenant: { name: "mixed", logo } },
      DEFAULT_IDENTITY,
    ),
  ).rejects.toThrow();
  await expect(
    parseTenantPresentation(body, { ...DEFAULT_IDENTITY, name: "Different" }),
  ).rejects.toThrow();
  for (const changed of [
    { ...body, schema_version: 2 },
    { ...body, tenant: null },
    { ...body, provider: {} },
    { ...body, tenant: { name: "x".repeat(81), logo } },
  ]) {
    await expect(
      parseTenantPresentation(changed, DEFAULT_IDENTITY),
    ).rejects.toThrow();
  }
});

test("a document retains complete A across identity-object refresh and rerenders", async () => {
  const body = await profile();
  const fetch = rs
    .spyOn(globalThis, "fetch")
    .mockImplementation(async () => Response.json(body));
  const app = render(
    <TenantPresentationProvider>
      <Probe />
    </TenantPresentationProvider>,
  );
  await screen.findByText("Example Tenant One");
  state.user = { ...state.user };
  fetch.mockImplementation(async () => Response.json(await profile("Revised")));
  app.rerender(
    <TenantPresentationProvider>
      <Probe />
    </TenantPresentationProvider>,
  );
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
  expect(screen.queryByText("Revised")).toBeNull();
  expect(fetch.mock.calls[0]?.[1]).toMatchObject({
    cache: "no-store",
    credentials: "include",
  });
});

test("unmounted account's late response cannot populate the next lifetime", async () => {
  let release: (response: Response) => void = () => undefined;
  const fetch = rs.spyOn(globalThis, "fetch").mockImplementationOnce(
    () =>
      new Promise<Response>((resolve) => {
        release = resolve;
      }),
  );
  const old = render(
    <TenantPresentationProvider>
      <Probe />
    </TenantPresentationProvider>,
  );
  old.unmount();
  state.user = { id: "two", system_role: "user" };
  fetch.mockImplementation(async () =>
    Response.json(await profile("Example Tenant Two")),
  );
  render(
    <TenantPresentationProvider>
      <Probe />
    </TenantPresentationProvider>,
  );
  await screen.findByText("Example Tenant Two");
  release(Response.json(await profile()));
  await waitFor(() =>
    expect(screen.queryByText("Example Tenant One")).toBeNull(),
  );
});

test.each([404, 503])(
  "fresh failed response %s refuses instead of showing generic workspace",
  async (status) => {
    rs.spyOn(globalThis, "fetch").mockResolvedValue(
      Response.json({ detail: "unavailable" }, { status }),
    );
    render(
      <TenantPresentationProvider>
        <Probe />
      </TenantPresentationProvider>,
    );
    await screen.findByText("Workspace identity unavailable");
    expect(screen.queryByText("legacy")).toBeNull();
  },
);

test("explicit legacy response preserves compatibility", async () => {
  rs.spyOn(globalThis, "fetch").mockResolvedValue(
    Response.json({ schema_version: 1, mode: "legacy" }),
  );
  render(
    <TenantPresentationProvider>
      <Probe />
    </TenantPresentationProvider>,
  );
  await screen.findByText("legacy");
});

test("oversized streamed responses are cancelled before admission", async () => {
  const cancelled = rs.fn();
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(384 * 1024 + 1));
    },
    cancel: cancelled,
  });
  rs.spyOn(globalThis, "fetch").mockResolvedValue(new Response(stream));
  render(
    <TenantPresentationProvider>
      <Probe />
    </TenantPresentationProvider>,
  );
  await screen.findByText("Workspace identity unavailable");
  expect(cancelled).toHaveBeenCalledTimes(1);
});
