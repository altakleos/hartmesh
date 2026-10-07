import { beforeEach, expect, it, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "/backend" }));

import { fetch } from "@/core/api/fetcher";
import { selectStorageSpacesEnabled } from "@/core/features/api";
import { createSpace, spaceFileURL, writeSpaceFile } from "@/core/spaces/api";

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
