import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";

import { expect, test } from "@playwright/test";

const MiB = 1024 * 1024;
const trials = [
  { query: "Quarterly archive", count: 1, first: "sample-0000" },
  { query: "Planning notes", count: 20, first: "sample-1999" },
  { query: "Planning notes 0123", count: 1, first: "sample-0123" },
  { query: "No matching title", count: 0, first: undefined },
];
const percentile95 = (samples: number[]) =>
  [...samples].sort((a, b) => a - b)[Math.ceil(samples.length * 0.95) - 1]!;

function limits(container: string) {
  const read = (name: string) =>
    execFileSync(
      "docker",
      ["exec", container, "cat", "/sys/fs/cgroup/" + name],
      {
        encoding: "utf8",
        timeout: 5000,
      },
    ).trim();
  return {
    memoryBytes: Number(read("memory.max")),
    swapBytes: Number(read("memory.swap.max")),
    pids: Number(read("pids.max")),
    cpu: read("cpu.max"),
  };
}

test("searches two thousand conversation titles through the signed-in gateway", async ({
  context,
  page,
}, info) => {
  const samples: {
    query: string;
    elapsedMs: number;
    status: number;
    count: number;
  }[] = [];
  const evidence: Record<string, unknown> = {
    samples,
    allowanceMs: 300,
    rounds: 15,
    percentile: "nearest rank, all 60 signed-in requests after warmup",
  };
  const persist = () =>
    writeFileSync(
      info.outputPath("title-search-observations.json"),
      JSON.stringify(evidence, null, 2) + "\n",
    );
  persist();
  const gateway = process.env.HARTMESH_TITLE_GATEWAY_ID;
  const postgres = process.env.HARTMESH_TITLE_POSTGRES_ID;
  if (
    !gateway ||
    !postgres ||
    ![gateway, postgres].every((id) => /^[a-f0-9]{12,64}$/.test(id))
  ) {
    throw new Error("Missing verified measurement container identity");
  }
  const gatewayLimits = limits(gateway);
  const postgresLimits = limits(postgres);
  evidence.limits = { gateway: gatewayLimits, postgres: postgresLimits };
  persist();
  const [quota, period] = gatewayLimits.cpu.split(" ").map(Number);
  expect(Number(quota) / Number(period)).toBe(2);
  expect(gatewayLimits).toMatchObject({
    memoryBytes: 1088 * MiB,
    swapBytes: 0,
    pids: 2048,
  });
  expect(postgresLimits).toMatchObject({
    memoryBytes: 768 * MiB,
    swapBytes: 0,
    pids: 512,
  });

  const request = context.request;
  const password = "synthetic-acceptance-password-123";
  const email = "search-reader@example.com";
  const admin = await request.post("/api/v1/auth/initialize", {
    data: { email: "admin@example.com", password },
  });
  expect(admin.status(), await admin.text()).toBe(201);
  const otherId = ((await admin.json()) as { id: string }).id;
  await context.clearCookies();
  const registered = await request.post("/api/v1/auth/register", {
    data: { email, password },
  });
  expect(registered.status(), await registered.text()).toBe(201);
  const ownerId = ((await registered.json()) as { id: string }).id;
  await context.clearCookies();
  const login = await request.post("/api/v1/auth/login/local", {
    form: { username: email, password },
  });
  expect(login.status()).toBe(200);
  const csrf = (await context.cookies()).find(
    (cookie) => cookie.name === "csrf_token",
  );
  if (!csrf) throw new Error("Missing signed-in CSRF cookie");
  const headers = { "X-CSRF-Token": csrf.value };
  const seed = JSON.parse(
    execFileSync(
      "docker",
      [
        "exec",
        "--workdir",
        "/app/backend",
        gateway,
        "/app/backend/.venv/bin/python",
        "/fixture/seed-title-search.py",
        ownerId,
        otherId,
      ],
      { encoding: "utf8", timeout: 60_000 },
    ),
  ) as {
    owned: number;
    foreign: number;
    synchronousCommit: string;
    walWriterDelay: string;
  };
  evidence.corpus = seed;
  persist();
  expect(seed).toEqual({
    owned: 2000,
    foreign: 1,
    synchronousCommit: "off",
    walWriterDelay: "200ms",
  });
  const features = await request.get("/api/features");
  expect(features.status()).toBe(200);
  expect(
    ((await features.json()) as { message_search?: boolean }).message_search,
  ).toBe(false);
  for (let round = 0; round <= 15; round++) {
    for (const trial of trials) {
      const start = performance.now();
      const reply = await request.post("/api/search", {
        headers,
        data: { query: trial.query, types: ["thread"], limit: 20 },
      });
      const body = await reply.body();
      const elapsedMs = performance.now() - start;
      const status = reply.status();
      const sample = { query: trial.query, elapsedMs, status, count: 0 };
      if (round > 0) samples.push(sample);
      persist();
      expect(status).toBe(200);
      const result = JSON.parse(body.toString("utf8")) as {
        items: { thread_id: string; type: string }[];
        messages_available: boolean;
      };
      sample.count = result.items.length;
      persist();
      expect(result.messages_available).toBe(false);
      expect(result.items).toHaveLength(trial.count);
      expect(result.items[0]?.thread_id).toBe(trial.first);
      expect(
        result.items.every(
          (hit) => hit.type === "thread" && hit.thread_id !== "sample-2000",
        ),
      ).toBe(true);
    }
  }
  const p95 = percentile95(samples.map((sample) => sample.elapsedMs));
  evidence.p95Ms = p95;
  evidence.byQueryP95Ms = Object.fromEntries(
    trials.map(({ query }) => [
      query,
      percentile95(
        samples
          .filter((item) => item.query === query)
          .map((item) => item.elapsedMs),
      ),
    ]),
  );
  persist();
  expect
    .soft(p95, "title search p95 measured through ingress")
    .toBeLessThanOrEqual(300);
  await page.goto("/workspace/chats");
  await expect(
    page.getByRole("link", { name: /Planning notes 1999/ }).first(),
  ).toBeVisible();
  const link = page.getByRole("link", { name: /Quarterly archive/ });
  await expect(link).toHaveCount(0);
  await page
    .getByRole("searchbox", { name: "Search conversation titles" })
    .fill("Quarterly archive");
  await expect(link).toBeVisible();
  await expect(link).toHaveAttribute("href", "/workspace/chats/sample-0000");
  await link.click();
  await expect(page).toHaveURL(/\/workspace\/chats\/sample-0000$/);
  evidence.openedOlderTitle = true;
  persist();
});
