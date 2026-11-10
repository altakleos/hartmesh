import { afterEach, expect, it, rs } from "@rstest/core";

import { listInstances } from "@/core/agent-instances/api";
import {
  readTelegramInstances,
  InstanceReadPool,
} from "@/core/telegram-bots/instance-reads";
rs.mock("@/core/agent-instances/api", () => ({
  listInstances: rs.fn(),
  isFullInstance: (value: object) => "permissions" in value,
}));
afterEach(() => {
  rs.clearAllMocks();
});
it("reads every page and preserves full entries for current Use filtering", async () => {
  const page = Array.from({ length: 100 }, (_, i) => ({
    id: String(i),
    permissions: i === 0 ? 0 : 1,
  }));
  rs.mocked(listInstances)
    .mockResolvedValueOnce({ instances: page } as never)
    .mockResolvedValueOnce({
      instances: [{ id: "last", permissions: 1 }, { id: "inventory" }],
    } as never);
  const result = await readTelegramInstances(
    "company",
    new AbortController().signal,
  );
  expect(result).toHaveLength(101);
  expect(result.at(-1)?.id).toBe("last");
  expect(rs.mocked(listInstances).mock.calls.map((call) => call[1])).toEqual([
    0, 100,
  ]);
});
it("retirement after a page prevents the next page and publishing partial data", async () => {
  const owner = new AbortController();
  rs.mocked(listInstances).mockImplementationOnce(async () => {
    owner.abort();
    return { instances: Array(100).fill({ id: "a", permissions: 1 }) } as never;
  });
  await expect(
    readTelegramInstances("personal", owner.signal),
  ).rejects.toMatchObject({ name: "AbortError" });
  expect(listInstances).toHaveBeenCalledTimes(1);
});
it("bounds reads and removes retired queued requests before admission", async () => {
  const pool = new InstanceReadPool(2);
  const owner = new AbortController();
  const next = new AbortController();
  const releases: (() => void)[] = [];
  let active = 0;
  let peak = 0;
  const entered: number[] = [];
  const read = (id: number, signal: AbortSignal) =>
    pool.read(signal, async () => {
      entered.push(id);
      active++;
      peak = Math.max(peak, active);
      await new Promise<void>((resolve) => releases.push(resolve));
      active--;
      return id;
    });
  const first = read(1, owner.signal);
  const second = read(2, owner.signal);
  const canceled = read(3, next.signal);
  const canceledResult = canceled.catch((error: unknown) => error);
  const fourth = read(4, owner.signal);
  await Promise.resolve();
  expect(entered).toEqual([1, 2]);
  next.abort();
  expect(await canceledResult).toMatchObject({ name: "AbortError" });
  releases.shift()!();
  await first;
  await Promise.resolve();
  expect(entered).toEqual([1, 2, 4]);
  expect(peak).toBe(2);
  releases.splice(0).forEach((release) => release());
  await Promise.all([second, fourth]);
});
