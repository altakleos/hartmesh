import { beforeEach, expect, test, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "/backend" }));
import { fetch } from "@/core/api/fetcher";
import { getPeople, searchPeople } from "@/core/people/api";

beforeEach(() => {
  rs.mocked(fetch).mockReset();
});

test("looks up repeated ids without treating them as an address search", async () => {
  const users = [
    { id: "a", email: "lee@example.com", name: null, active: false },
  ];
  rs.mocked(fetch).mockResolvedValue(Response.json({ users }));
  expect(await getPeople(["a", "b"])).toEqual({ users });
  const input = rs.mocked(fetch).mock.calls[0]![0];
  const query = new URL(
    typeof input === "string" ? input : input.url,
    "http://localhost",
  ).searchParams;
  expect(query.getAll("ids")).toEqual(["a", "b"]);
  expect(query.has("query")).toBe(false);
  expect(query.has("limit")).toBe(false);
});

test("searches the operator's directory without claiming an account identity", async () => {
  rs.mocked(fetch).mockResolvedValue(Response.json({ users: [] }));
  await searchPeople("lee+team@example.com", undefined, 30);
  const [url, init] = rs.mocked(fetch).mock.calls[0]!;
  const query = new URL(
    typeof url === "string" ? url : url.url,
    "http://localhost",
  ).searchParams;
  expect(query.get("query")).toBe("lee+team@example.com");
  expect(query.get("limit")).toBe("30");
  expect(query.has("ids")).toBe(false);
  expect(init?.method).toBeUndefined();
});

test("passes directory validation failures to the caller as HTTP errors", async () => {
  rs.mocked(fetch).mockResolvedValue(
    Response.json(
      { detail: "Search text is at most 64 characters" },
      { status: 400 },
    ),
  );
  await expect(searchPeople("x".repeat(65))).rejects.toMatchObject({
    status: 400,
  });
});
