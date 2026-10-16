import { beforeEach, expect, test, rs } from "@rstest/core";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "/backend" }));
import { readWorkRun } from "@/core/agent-instances/work-run";
import { fetch } from "@/core/api/fetcher";

beforeEach(() => {
  rs.mocked(fetch).mockReset();
});
test("reads the exact captured run and returns only the ending protocol field", async () => {
  rs.mocked(fetch).mockResolvedValue(
    Response.json({
      thread_id: "thread/id",
      run_id: "run/id",
      stop_reason: "busy",
      private_diagnostic: "SECRET",
    }),
  );
  expect(await readWorkRun("thread/id", "run/id")).toEqual({
    stop_reason: "busy",
  });
  expect(rs.mocked(fetch).mock.calls[0]![0]).toBe(
    "/backend/api/threads/thread%2Fid/runs/run%2Fid",
  );
});
test("a previous server's missing ending means no known reason", async () => {
  rs.mocked(fetch).mockResolvedValue(
    Response.json({ thread_id: "thread", run_id: "run" }),
  );
  expect(await readWorkRun("thread", "run")).toEqual({ stop_reason: null });
});
test.each([
  { thread_id: "other", run_id: "run", stop_reason: "busy" },
  { thread_id: "thread", run_id: "other", stop_reason: "busy" },
  { thread_id: "thread", run_id: "run", stop_reason: { private: "secret" } },
])("rejects a mismatched or malformed run", async (body) => {
  rs.mocked(fetch).mockResolvedValue(Response.json(body));
  await expect(readWorkRun("thread", "run")).rejects.toThrow(
    "Run could not be read",
  );
});
test("retired reads cannot return late run details", async () => {
  let resolve!: (value: Response) => void;
  const response = new Promise<Response>((done) => {
    resolve = done;
  });
  rs.mocked(fetch).mockReturnValue(response);
  const owner = new AbortController();
  const reading = readWorkRun("thread", "run", owner.signal);
  const rejected = expect(reading).rejects.toMatchObject({
    name: "AbortError",
  });
  owner.abort();
  resolve(
    Response.json({ thread_id: "thread", run_id: "run", stop_reason: "busy" }),
  );
  await rejected;
});
