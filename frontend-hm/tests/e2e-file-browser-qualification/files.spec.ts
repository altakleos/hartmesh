import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { availableParallelism } from "node:os";
import path from "node:path";

import {
  expect,
  test,
  type APIRequestContext,
  type Page,
  type Response,
} from "@playwright/test";

test.use({ viewport: { width: 1440, height: 900 } });

type Area = "personal" | "shared";
const endpoint = { personal: "/api/files", shared: "/api/shared" };
const title = { personal: "My files", shared: "Company shared" };
const urls = {
  personal: "/workspace/files",
  shared: "/workspace/files?tab=shared",
};
const p95 = (samples: number[]) =>
  [...samples].sort((a, b) => a - b)[Math.ceil(samples.length * 0.95) - 1]!;
const time = () => performance.now();

async function authenticatedAccounts(
  request: APIRequestContext,
  clear: () => Promise<void>,
) {
  const password = "synthetic-files-acceptance-2026";
  const admin = await request.post("/api/v1/auth/initialize", {
    data: { email: "admin@example.com", password },
  });
  expect(admin.status(), await admin.text()).toBe(201);
  const other = ((await admin.json()) as { id: string }).id;
  await clear();
  const email = "files-reader@example.com";
  const registered = await request.post("/api/v1/auth/register", {
    data: { email, password },
  });
  expect(registered.status(), await registered.text()).toBe(201);
  const user = ((await registered.json()) as { id: string }).id;
  await clear();
  const login = await request.post("/api/v1/auth/login/local", {
    form: { username: email, password },
  });
  expect(login.status()).toBe(200);
  return { user, other };
}

type Timings = {
  rawListMs: number[];
  documentMs: number[];
  firstFolderPageMs: number[];
  childFolderMs: number[];
};

const timings = (): Timings => ({
  rawListMs: [],
  documentMs: [],
  firstFolderPageMs: [],
  childFolderMs: [],
});

function browserRegion(page: Page, area: Area) {
  return page.getByRole("region", { name: title[area], exact: true });
}

function waitForFolder(page: Page, area: Area, path: string) {
  return page.waitForResponse((response) => {
    const url = new URL(response.url());
    return (
      url.pathname === `/api/file-browser/v1/${area}/folder` &&
      url.searchParams.get("path") === path
    );
  });
}

async function checkFolder(response: Response, path: string) {
  expect(response.status()).toBe(200);
  const data = (await response.json()) as {
    files: { kind: string; name: string; path: string }[];
    truncated: boolean;
  };
  expect(data.truncated).toBe(false);
  expect(data.files).toHaveLength(path ? 400 : 26);
  if (path) {
    expect(
      data.files.every(
        (entry) =>
          entry.kind === "file" && entry.path === path + "/" + entry.name,
      ),
    ).toBe(true);
  } else {
    expect(
      data.files.filter((entry) => entry.kind === "directory"),
    ).toHaveLength(25);
    expect(data.files.find((entry) => entry.path === "visible.txt")?.kind).toBe(
      "file",
    );
  }
}

async function navigateFromWorkspace(page: Page, area: Area) {
  const previous = area === "personal" ? "shared" : "personal";
  const origin = browserRegion(page, previous);
  const location = origin.getByRole("link", { name: title[area], exact: true });
  await location.waitFor();
  const start = time();
  const folder = waitForFolder(page, area, "");
  await location.click();
  const region = browserRegion(page, area);
  const group = region.getByRole("button", {
    name: "Collection 00",
    exact: true,
  });
  await group.waitFor();
  const elapsed = time() - start;
  await checkFolder(await folder, "");
  return { elapsed, region, group };
}
test("bounded files stay scoped and usable across supported image pairings", async ({
  page,
  context,
}) => {
  const gateway = process.env.HARTMESH_FILES_GATEWAY_ID,
    pair = process.env.HARTMESH_FILES_PAIR;
  const out = process.env.HARTMESH_ACCEPTANCE_ARTIFACTS;
  if (!gateway || !/^[a-f0-9]{64}$/.test(gateway) || !out || !pair)
    throw new Error("Use the disposable file-browser qualification profile");
  const currentUI = pair !== "previous-ui",
    currentGateway = pair !== "previous-gateway";
  const areas: Area[] = ["personal", "shared"];
  const desktopBrowser = currentUI && currentGateway;
  const samples = { personal: timings(), shared: timings() };
  const evidence = {
    pair,
    host: { architecture: process.arch, logicalCpus: availableParallelism() },
    viewport: page.viewportSize(),
    seed: 0,
    trials: samples,
    p95: {} as Partial<
      Record<
        Area,
        { firstFolderPageMs: number; childFolderMs: number; rawListMs: number }
      >
    >,
    concurrency: [] as {
      area: Area;
      rawListMs: number;
      chatMs: number;
      overlapMs: number;
    }[],
    sizes: { personal: 0, shared: 0 },
    cgroup: {} as Record<string, string>,
  };
  mkdirSync(out, { recursive: true });
  const save = () =>
    writeFileSync(
      path.join(out, "bounded-file-browser.json"),
      JSON.stringify(evidence, null, 2),
    );
  save();
  const request = context.request;
  const actors = await authenticatedAccounts(request, () =>
    context.clearCookies(),
  );
  const seedSource = readFileSync(
    path.resolve("../docker/acceptance/file-browser/seed.py"),
    "utf8",
  );
  const seed = JSON.parse(
    execFileSync(
      "docker",
      [
        "exec",
        "-i",
        "--workdir",
        "/app/backend",
        gateway,
        "/app/backend/.venv/bin/python",
        "-",
        actors.user,
        actors.other,
        desktopBrowser ? "large" : "small",
      ],
      { input: seedSource, encoding: "utf8", timeout: 90000 },
    ),
  ) as { per_location: number };
  evidence.seed = seed.per_location;
  expect(seed.per_location).toBe(desktopBrowser ? 10001 : 2);
  const readCgroup = (field: string) =>
    execFileSync(
      "docker",
      ["exec", gateway, "cat", "/sys/fs/cgroup/" + field],
      { encoding: "utf8", timeout: 5000 },
    ).trim();
  for (const field of ["cpu.max", "memory.max", "memory.swap.max", "pids.max"])
    evidence.cgroup[field] = readCgroup(field);
  const [quota, period] = evidence.cgroup["cpu.max"]!.split(" ").map(Number);
  expect(quota! / period!).toBe(2);
  expect(Number(evidence.cgroup["memory.max"])).toBe(1088 * 1024 * 1024);
  expect(Number(evidence.cgroup["memory.swap.max"])).toBe(0);
  expect(Number(evidence.cgroup["pids.max"])).toBe(2048);
  evidence.cgroup.memoryBefore = readCgroup("memory.current");
  // Record the observed peak even when a later usability assertion fails.
  save();
  try {
    const discovery = await request.get("/api/features");
    expect(discovery.ok()).toBe(true);
    const features = (await discovery.json()) as {
      file_browser?: { storage_mode: string; operations: { shared: string[] } };
    };
    if (currentGateway) {
      expect(features.file_browser?.storage_mode).toBe("ordinary");
      expect(features.file_browser?.operations.shared).toContain("select");
      expect(features.file_browser?.operations.shared).toContain("folder");
      expect(features.file_browser?.operations.shared).not.toContain("remove");
    } else expect(features.file_browser).toBeUndefined();
    const csrf = (await context.cookies()).find(
      (x) => x.name === "csrf_token",
    )!;
    const headers = { "X-CSRF-Token": csrf.value };
    for (const area of areas) {
      const result = await request.get(
        `/api/file-browser/v1/${area}/selection`,
        {
          params: { path: "Collection 00/Report 0000.txt" },
        },
      );
      expect(result.status()).toBe(currentGateway ? 200 : 404);
      const before = await request.get(
        endpoint[area] + "/Collection%2000/Report%200000.txt",
      );
      expect(before.status()).toBe(200);
      expect(await before.text()).toBe(`${area} sample\n`);
    }
    const foreign = await request.get("/api/files/other-account.txt");
    expect(foreign.status()).toBe(404);
    const trialThread = await request.post("/api/langgraph/threads", {
      headers,
      data: { metadata: {} },
    });
    expect(trialThread.ok()).toBe(true);
    const { thread_id: thread } = (await trialThread.json()) as {
      thread_id: string;
    };
    async function chat() {
      const start = time();
      const result = await request.post(
        `/api/langgraph/threads/${thread}/runs/stream`,
        {
          headers,
          data: {
            assistant_id: "lead_agent",
            input: {
              messages: [{ type: "human", content: "acceptance:timing" }],
            },
            context: {
              mode: "flash",
              model_name: "acceptance-model",
              thinking_enabled: false,
              subagent_enabled: false,
            },
            stream_mode: ["values"],
          },
        },
      );
      expect(result.status(), await result.text()).toBe(200);
      expect(await result.text()).toContain("Timing sample complete.");
      return [start, time()] as const;
    }
    async function rawList(area: Area) {
      const start = time();
      const reply = await request.get(endpoint[area]);
      expect(reply.status()).toBe(200);
      const data = (await reply.json()) as {
        files: { path: string }[];
        truncated: boolean;
      };
      const end = time();
      samples[area].rawListMs.push(end - start);
      evidence.sizes[area] = data.files.length;
      expect(data.files).toHaveLength(Math.min(seed.per_location, 10000));
      expect(data.truncated).toBe(desktopBrowser);
      expect(data.files.some((x) => x.path === "other-account.txt")).toBe(
        false,
      );
      return [start, end] as const;
    }
    for (const area of areas) {
      const [list, turn] = await Promise.all([rawList(area), chat()]);
      const overlap = Math.min(list[1], turn[1]) - Math.max(list[0], turn[0]);
      evidence.concurrency.push({
        area,
        rawListMs: list[1] - list[0],
        chatMs: turn[1] - turn[0],
        overlapMs: overlap,
      });
      save();
      expect(overlap).toBeGreaterThan(0);
      if (!desktopBrowser) {
        const nested = new URL(urls[area], "http://localhost");
        nested.searchParams.set("folder", "Collection 00");
        await page.goto(nested.pathname + nested.search);
        const classic = page.getByTestId(
          area === "personal" ? "my-files-list" : "shared-files-list",
        );
        await expect(
          classic.getByRole("link", { name: "visible.txt", exact: true }),
        ).toBeVisible();
        const row = classic.getByRole("link", {
          name: "Report 0000.txt",
          exact: true,
        });
        await expect(row).toBeVisible();
        const href = await row.getAttribute("href");
        expect(href).toBeTruthy();
        const link = new URL(href!, page.url());
        expect(link.origin).toBe(new URL(page.url()).origin);
        expect(decodeURIComponent(link.pathname)).toBe(
          endpoint[area] + "/Collection 00/Report 0000.txt",
        );
        const opened = await request.get(link.toString());
        expect(opened.status()).toBe(200);
        expect(await opened.text()).toBe(`${area} sample\n`);
        await expect(
          page.getByRole("button", { name: /More actions for/ }),
        ).toHaveCount(0);
      }
    }
    if (!desktopBrowser) return;
    const flatReads = { personal: 0, shared: 0 };
    page.on("request", (request) => {
      const requested = new URL(request.url()).pathname;
      for (const area of areas)
        if (requested === endpoint[area]) flatReads[area]++;
    });
    // A direct document load includes shell startup. Record it independently from
    // entering a location inside the signed-in workspace and opening a child.
    for (const area of areas) {
      const root = waitForFolder(page, area, "");
      const start = time();
      await page.goto(urls[area], { waitUntil: "commit" });
      await browserRegion(page, area)
        .getByRole("button", { name: "Collection 00", exact: true })
        .waitFor();
      samples[area].documentMs.push(time() - start);
      await checkFolder(await root, "");
      expect(flatReads[area]).toBe(0);
      save();
    }
    // Alternate real locations, starting from Shared. Each measurement requires
    // the requested area's folder control to be usable after the navigation.
    for (let i = 0; i < 20; i++) {
      for (const area of areas) {
        const { elapsed, region, group } = await navigateFromWorkspace(
          page,
          area,
        );
        samples[area].firstFolderPageMs.push(elapsed);
        const child = waitForFolder(page, area, "Collection 00");
        const start = time();
        await group.click();
        await region
          .getByRole("link", { name: /^Report [0-9]+\.txt$/ })
          .first()
          .waitFor();
        samples[area].childFolderMs.push(time() - start);
        await checkFolder(await child, "Collection 00");
        if (i === 0) {
          expect(flatReads[area]).toBe(0);
          const search = region.getByRole("searchbox");
          const flat = page.waitForResponse(
            (response) => new URL(response.url()).pathname === endpoint[area],
          );
          await search.fill("Report");
          expect((await flat).status()).toBe(200);
          await expect(
            region.getByText(/only the first 10[,]?000 entries/i),
          ).toBeVisible();
          await search.fill("");
          await region
            .getByRole("link", { name: /^Report [0-9]+\.txt$/ })
            .first()
            .waitFor();
        }
        if (i % 4 === 0) await rawList(area);
        save();
      }
    }
    for (const area of areas) {
      const sample = samples[area];
      evidence.p95[area] = {
        firstFolderPageMs: p95(sample.firstFolderPageMs),
        childFolderMs: p95(sample.childFolderMs),
        rawListMs: p95(sample.rawListMs),
      };
    }
    save();
    for (const area of areas) {
      expect(flatReads[area]).toBe(1);
      expect(evidence.p95[area]!.firstFolderPageMs).toBeLessThan(1000);
      expect(evidence.p95[area]!.childFolderMs).toBeLessThan(1000);
    }
  } finally {
    evidence.cgroup.memoryAfter = readCgroup("memory.current");
    evidence.cgroup.peakMemory = readCgroup("memory.peak");
    save();
  }
});
