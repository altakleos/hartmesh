import { beforeEach, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "/backend" }));

import { fetch } from "@/core/api/fetcher";
import { selectStorageSpacesEnabled } from "@/core/features/api";
import {
  changeSpaceLifecycle,
  retireSpaceAttachments,
  createSpace,
  spaceFileURL,
  writeSpaceFile,
} from "@/core/spaces/api";

const mocked = rs.mocked(fetch);
const SPACE = "a".repeat(32);

beforeEach(() => {
  mocked.mockReset();
});

it("uses stable resource-relative file references without a chat", () => {
  expect(spaceFileURL(SPACE, "notes/Q3 #2.md", true)).toBe(
    `/backend/api/spaces/${SPACE}/content?path=notes%2FQ3+%232.md&download=true`,
  );
  expect(() => spaceFileURL("../owner", "x")).toThrow();
});

it("provisions personal custody without sending a claimed actor", async () => {
  mocked.mockResolvedValueOnce(
    new Response(JSON.stringify({ id: SPACE }), { status: 201 }),
  );
  await createSpace("Home", "personal");
  expect(JSON.parse(mocked.mock.calls[0]![1]!.body as string)).toEqual({
    name: "Home",
    custody: "personal",
  });
});

it("requires generation and editor revision and never automatically retries an uncertain save", async () => {
  mocked.mockResolvedValueOnce(
    new Response(
      JSON.stringify({ detail: "Operation pending; recovery required" }),
      { status: 409 },
    ),
  );
  await expect(
    writeSpaceFile(SPACE, "page.md", "changed", {
      generation: 7,
      expectedSha256: "b".repeat(64),
    }),
  ).rejects.toThrow("Operation pending");
  expect(mocked).toHaveBeenCalledTimes(1);
  const [url, init] = mocked.mock.calls[0]!;
  const params = new URL(url as string, "http://test").searchParams;
  expect(params.get("generation")).toBe("7");
  expect(params.get("expected_sha256")).toBe("b".repeat(64));
  expect(params.get("operation_id")).toMatch(/^[0-9a-f]{32}$/);
  expect(init?.method).toBe("PUT");
});

it("offers storage only after the host reports an available capability", () => {
  expect(selectStorageSpacesEnabled({ agents_api: { enabled: false } })).toBe(
    false,
  );
  expect(
    selectStorageSpacesEnabled({
      agents_api: { enabled: false },
      storage_spaces: { enabled: true },
    }),
  ).toBe(true);
});

it("binds lifecycle to resource generation and a unique operation without claimed authority", async () => {
  mocked.mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));
  await changeSpaceLifecycle(SPACE, 7, "restore", "b".repeat(32));
  const [url, init] = mocked.mock.calls[0]!;
  expect(url).toBe(`/backend/api/spaces/${SPACE}/lifecycle`);
  const body = JSON.parse(init!.body as string);
  expect(body).toEqual({
    action: "restore",
    generation: 7,
    backup_id: "b".repeat(32),
    operation_id: expect.stringMatching(/^[0-9a-f]{32}$/),
  });
});

it("retirement sends the captured attachment IDs instead of ambient current environments", async () => {
  mocked.mockResolvedValue(
    new Response(JSON.stringify({ complete: true }), { status: 200 }),
  );
  await retireSpaceAttachments(SPACE, 7, ["b".repeat(32)]);
  expect(JSON.parse(mocked.mock.calls[0]![1]!.body as string)).toEqual({
    generation: 7,
    attachment_ids: ["b".repeat(32)],
  });
});
