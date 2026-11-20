import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";

import { expect, test } from "@playwright/test";

const mebibyte = 1024 * 1024;
const trials = 5;
const quietWindowMs = 61_000;
const fixtureURL = (filename: string) =>
  new URL("../fixtures/" + filename, import.meta.url);
const fixture = readFileSync(
  fixtureURL("business-report/2026-08-business-review.report.json"),
);
const office = [
  {
    kind: "workbook",
    extension: "xlsx",
    visible: "Apples",
    module: "workbooks.project_workbook",
  },
  {
    kind: "document",
    extension: "docx",
    visible: "Opening paragraph.",
    module: "documents.project_document",
  },
  {
    kind: "deck",
    extension: "pptx",
    visible: "Discuss the schedule.",
    module: "slides.project_deck",
  },
].map((sample) => ({
  ...sample,
  filename: "sample." + sample.extension,
  source: readFileSync(fixtureURL("office/sample." + sample.extension)),
  expected: JSON.parse(
    readFileSync(fixtureURL("office/" + sample.kind + ".json"), "utf8"),
  ) as { blocks: { type: string; text?: string }[] },
}));
const sha256 = (bytes: Buffer) =>
  createHash("sha256").update(bytes).digest("hex");
const numberList = (count: number) =>
  Array.from({ length: count }, (_, i) => i);
const median = (values: number[]) =>
  [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]!;

test("measure previews in a signed-in ordinary chat", async ({
  context,
  page,
}, info) => {
  const timings: {
    operation: string;
    elapsedMs: number | null;
    completed: boolean;
  }[] = [];
  const evidence: Record<string, unknown> = {
    timings,
    unverified: [
      "Kernel victim selection during container-wide memory exhaustion",
      "Unbounded projector workloads and adversarial Office documents",
    ],
  };
  const persist = () =>
    writeFileSync(
      info.outputPath("preview-observations.json"),
      JSON.stringify(evidence, null, 2) + "\n",
    );
  persist();
  async function timed(operation: string, action: () => Promise<void>) {
    const sample: (typeof timings)[number] = {
      operation,
      elapsedMs: null,
      completed: false,
    };
    timings.push(sample);
    persist();
    const started = performance.now();
    try {
      await action();
      sample.completed = true;
    } finally {
      sample.elapsedMs = performance.now() - started;
      persist();
    }
    return sample.elapsedMs;
  }
  const gateway = process.env.HARTMESH_PREVIEW_GATEWAY_ID;
  if (!gateway || !/^[a-f0-9]{12,64}$/.test(gateway)) {
    throw new Error("Missing verified Gateway container identity");
  }
  const status = execFileSync(
    "docker",
    ["exec", gateway, "cat", "/proc/1/status"],
    { encoding: "utf8", timeout: 5000 },
  );
  const gatewayPrivileges = {
    uids: /^Uid:\s*(.+)$/m.exec(status)?.[1]?.trim().split(/\s+/).map(Number),
    effectiveCapabilities: /^CapEff:\s*(\w+)$/m.exec(status)?.[1],
    noNewPrivileges: /^NoNewPrivs:\s*(\d+)$/m.exec(status)?.[1],
  };
  evidence.gatewayPrivileges = gatewayPrivileges;
  persist();
  expect(gatewayPrivileges.uids).toEqual([1000, 1000, 1000, 1000]);
  expect(gatewayPrivileges.effectiveCapabilities).toMatch(/^0+$/);
  expect(gatewayPrivileges.noNewPrivileges).toBe("1");
  const cgroup = (...names: string[]) =>
    execFileSync(
      "docker",
      [
        "exec",
        gateway,
        "cat",
        ...names.map((name) => "/sys/fs/cgroup/" + name),
      ],
      { encoding: "utf8", timeout: 5000 },
    ).trim();
  const [memory, swap, pids, cpu] = cgroup(
    "memory.max",
    "memory.swap.max",
    "pids.max",
    "cpu.max",
  ).split("\n");
  const [quota, period] = (cpu ?? "").split(" ");
  const limits = {
    memoryBytes: Number(memory),
    swapBytes: Number(swap),
    pids: Number(pids),
    cpus: Number(quota) / Number(period),
  };
  evidence.containerLimits = limits;
  persist();
  expect(limits).toEqual({
    memoryBytes: 1088 * mebibyte,
    swapBytes: 0,
    pids: 2048,
    cpus: 2,
  });
  const memoryPeak = () => {
    const bytes = Number(cgroup("memory.peak"));
    expect(Number.isSafeInteger(bytes) && bytes > 0).toBe(true);
    return bytes;
  };
  const runningWorkers = () =>
    execFileSync("docker", ["top", gateway, "-eo", "pid,etimes,rss,vsz,args"], {
      encoding: "utf8",
      timeout: 3000,
    })
      .split("\n")
      .filter((line) => line.includes("projection_worker.py project "))
      .map((line) => {
        const [pid, ageSeconds, rssKiB, virtualKiB] = line
          .trimStart()
          .split(/\s+/, 4)
          .map(Number);
        return { pid, ageSeconds, rssKiB, virtualKiB };
      });
  const gatewayIdentity = () =>
    execFileSync(
      "docker",
      [
        "inspect",
        "--format",
        "{{.State.Pid}} {{.State.StartedAt}} {{.RestartCount}}",
        gateway,
      ],
      { encoding: "utf8", timeout: 5000 },
    ).trim();
  const password = "synthetic-acceptance-password-123";
  const credentials = { email: "preview@example.com", password };
  const request = context.request;
  await request
    .post("/api/v1/auth/initialize", {
      data: { email: "admin@example.com", password },
    })
    .then(async (response) => {
      expect(response.status(), await response.text()).toBe(201);
    });
  await context.clearCookies();
  await request
    .post("/api/v1/auth/register", { data: credentials })
    .then(async (response) => {
      expect(response.status(), await response.text()).toBe(201);
    });
  await context.clearCookies();
  const login = await request.post("/api/v1/auth/login/local", {
    form: { username: credentials.email, password },
  });
  expect(login.ok(), await login.text()).toBe(true);
  const csrf = (await context.cookies()).find(
    (cookie) => cookie.name === "csrf_token",
  );
  if (!csrf) throw new Error("Missing CSRF cookie from authenticated login");
  const headers = { "X-CSRF-Token": csrf.value };
  async function createThread() {
    const created = await request.post("/api/langgraph/threads", {
      headers,
      data: { metadata: {} },
    });
    expect(created.ok(), await created.text()).toBe(true);
    return ((await created.json()) as { thread_id: string }).thread_id;
  }
  async function turn(thread: string, prompt: string, operation: string) {
    return timed(operation, async () => {
      const response = await request.post(
        "/api/langgraph/threads/" + thread + "/runs/wait",
        {
          headers,
          data: {
            assistant_id: "lead_agent",
            input: { messages: [{ role: "user", content: prompt }] },
          },
        },
      );
      const body = await response.text();
      expect(response.ok(), body).toBe(true);
      expect(body).toContain(
        prompt === "acceptance:timing"
          ? "Timing sample complete."
          : "Preview files ready.",
      );
    });
  }
  async function list(operation: string) {
    return timed(operation, async () => {
      const response = await request.get("/api/files");
      expect(response.ok(), await response.text()).toBe(true);
    });
  }

  try {
    const thread = await createThread();
    const originals = new Map<string, Buffer>([
      ["sample.report.json", fixture],
    ]);
    for (const { filename, source } of office) {
      originals.set(filename, source);
    }
    for (const [name, buffer] of originals) {
      const upload = await request.post("/api/threads/" + thread + "/uploads", {
        headers,
        multipart: {
          files: {
            name,
            mimeType: name.endsWith(".json")
              ? "application/json"
              : "application/octet-stream",
            buffer,
          },
        },
      });
      expect(upload.ok(), await upload.text()).toBe(true);
    }
    await turn(thread, "acceptance:preview", "produce-fixtures");
    await turn(await createThread(), "acceptance:timing", "warm-chat");
    await list("warm-list");
    const chat: number[] = [];
    const listing: number[] = [];
    for (let i = 0; i < trials; i++) {
      chat.push(
        await turn(await createThread(), "acceptance:timing", "baseline-chat"),
      );
      listing.push(await list("baseline-list"));
    }
    const peak = memoryPeak();
    evidence.beforeFirstPreview = {
      chatMs: chat,
      listingMs: listing,
      peakBytes: peak,
    };
    expect.soft(peak + 288 * mebibyte).toBeLessThanOrEqual(limits.memoryBytes);
    const base = "/api/threads/" + thread + "/artifacts/mnt/user-data/outputs/";
    const sample = (name: string, timeout?: number) =>
      request.get(base + name + "?preview=example.measurement%2Fsample", {
        timeout,
      });
    const gatewayLogs = () => {
      const logs = spawnSync("docker", ["logs", gateway], {
        encoding: "utf8",
        timeout: 5000,
      });
      expect(logs.status).toBe(0);
      return (logs.stdout + logs.stderr).split("\n");
    };
    const workerLog = gatewayLogs().filter((line) =>
      /Preview worker (loaded |self-test complete:)/.test(line),
    );
    evidence.workerSelfTest = workerLog;
    persist();
    expect(workerLog.join("\n")).toMatch(
      /Preview worker loaded hartmesh_legacy_report\.projection\.project_report /,
    );
    for (const { module } of office) {
      expect(workerLog.join("\n")).toContain(
        "Preview worker loaded hartmesh_office_preview." + module + " ",
      );
    }
    expect(workerLog.join("\n")).toContain(
      "Preview worker self-test complete: 5/5 projectors available",
    );

    await page.goto("/workspace/chats/" + thread);
    await page.getByTestId("artifact-trigger").click();
    const panel = page.getByRole("complementary");
    await panel.getByText("sample.report.json", { exact: true }).click();
    await expect(
      page.getByTestId("plugin-artifact-presentation"),
    ).toContainText("August 2026 Business Review");
    const reportProjection = await request.get(
      base + "sample.report.json?preview=hartmesh.legacy-report%2Freport",
    );
    expect(reportProjection.headers()["x-artifact-projection"]).toBe(
      "business-report-v1",
    );
    expect(await reportProjection.json()).not.toHaveProperty("meta.build");
    expect(
      await (
        await request.get(base + "sample.report.json?download=true")
      ).body(),
    ).toEqual(fixture);
    evidence.report = {
      status: reportProjection.status(),
      sourceBytes: fixture.length,
    };

    const officeEvidence: Record<string, unknown>[] = [];
    evidence.office = officeEvidence;
    let selected = "sample.report.json";
    const openSelector = () =>
      page.getByRole("combobox").filter({ hasText: selected }).click();
    const artifactPanel = page.locator("#artifacts");
    for (const { kind, filename, source, expected, visible } of office) {
      const digest = sha256(source);
      const notice = expected.blocks.find(
        (block) => block.type === "notice",
      )?.text;
      if (!notice)
        throw new Error("Fixture lacks the expected omission notice");
      const entry: Record<string, unknown> = {
        filename,
        sourceBytes: source.length,
        sourceSha256: digest,
      };
      officeEvidence.push(entry);
      const elapsed = await timed(kind + "-projection", async () => {
        const projected = await request.get(
          base + filename + "?preview=hartmesh.office-preview%2F" + kind,
        );
        entry.status = projected.status();
        const bytes = await projected.body();
        entry.previewBytes = bytes.length;
        entry.previewSha256 = sha256(bytes);
        expect(projected.status()).toBe(200);
        expect(projected.headers()["x-artifact-projection"]).toBe(
          "office-" + kind + "-v1",
        );
        expect(projected.headers().etag).toBe('"' + digest + '"');
        expect(Number(projected.headers()["x-artifact-source-bytes"])).toBe(
          source.length,
        );
        expect(source.length).toBeLessThanOrEqual(16 * mebibyte);
        expect(bytes.length).toBeLessThanOrEqual(mebibyte);
        expect(JSON.parse(bytes.toString("utf8"))).toEqual(expected);
      });
      expect.soft(elapsed).toBeLessThan(1000);
      await openSelector();
      await page.getByRole("option", { name: filename, exact: true }).click();
      selected = filename;
      const view = artifactPanel.getByTestId("artifact-view");
      await expect(view.getByText(visible, { exact: true })).toBeVisible();
      await expect(view.getByText(notice, { exact: true })).toBeVisible();
      await view.screenshot({ path: info.outputPath(kind + ".png") });
      const downloading = page.waitForEvent("download");
      await artifactPanel
        .getByRole("button", { name: "Download", exact: true })
        .click();
      const download = await downloading;
      expect(download.suggestedFilename()).toBe(filename);
      entry.downloadSha256 = sha256(readFileSync(await download.path()));
      expect(entry.downloadSha256).toBe(digest);
      persist();
    }

    const overflow = await sample("large.sample");
    expect(overflow.status()).toBe(413);
    const invalid = await sample("invalid.sample");
    expect(invalid.status()).toBe(422);
    const original = gatewayIdentity();
    const interrupted = await sample("interrupted.sample");
    expect(interrupted.status()).toBe(503);
    expect(gatewayIdentity()).toBe(original);
    expect((await request.get("/health/ready", { timeout: 5000 })).ok()).toBe(
      true,
    );
    expect(runningWorkers()).toHaveLength(0);
    evidence.finiteFailures = {
      outputLimit: overflow.status(),
      invalidInput: invalid.status(),
      interruptedChild: interrupted.status(),
      gatewayIdentity: original,
    };
    const memoryEvidence: Record<string, unknown> = {
      reservationBytes: 256 * mebibyte,
      protection: "none",
    };
    evidence.childAddressSpace = memoryEvidence;
    await timed("child-address-space", async () => {
      const address = await sample("address.sample");
      memoryEvidence.status = address.status();
      expect(address.status()).toBe(200);
      memoryEvidence.observed = await address.json();
      // Linux ENOMEM from one finite mapping. The callback reports the actual errno.
      expect(memoryEvidence.observed).toEqual({
        soft: 256 * mebibyte,
        hard: 256 * mebibyte,
        errno: 12,
      });
    });
    const concurrentChat: number[] = [];
    const workerSamples: ReturnType<typeof runningWorkers>[] = [];
    evidence.duringFiniteWorker = { chatMs: concurrentChat, workerSamples };
    for (let i = 0; i < trials; i++) {
      const fresh = await createThread();
      const [projection, turnMs] = await Promise.all([
        sample("turn-" + i + ".sample"),
        (async () => {
          await expect
            .poll(
              () => {
                const workers = runningWorkers();
                workerSamples.push(workers);
                return workers.length;
              },
              { intervals: [25], timeout: 3000 },
            )
            .toBe(1);
          return turn(fresh, "acceptance:timing", "chat-during-finite-worker");
        })(),
      ]);
      concurrentChat.push(turnMs);
      expect(projection.status()).toBe(200);
    }
    evidence.duringFiniteWorker = {
      chatMs: concurrentChat,
      workerSamples,
      chatMedianRatio: median(concurrentChat) / median(chat),
    };
    expect.soft(median(concurrentChat)).toBeLessThanOrEqual(1.1 * median(chat));

    await timed(
      "quiet-before-burst",
      () => new Promise((resolve) => setTimeout(resolve, quietWindowMs)),
    );
    expect(runningWorkers()).toHaveLength(0);
    await openSelector();
    const report = page.getByRole("option", {
      name: "busy.report.json",
      exact: true,
    });
    await expect(report).toBeVisible();
    const during: number[] = [];
    const overlaps: { beforePid: number; afterPids: (number | undefined)[] }[] =
      [];
    const statuses: (number | null)[] = Array(12).fill(null);
    const burst = {
      quietWindowMs,
      count: statuses.length,
      statuses,
      readiness: 0,
      listingMs: during,
      overlaps,
    };
    evidence.parallelReads = burst;
    const requests = numberList(statuses.length).map(async (i) => {
      const response = await sample("delay-" + i + ".sample");
      statuses[i] = response.status();
      persist();
      return response;
    });
    // Wait for an actual refusal before trying the file in the browser.
    const firstBusy = await Promise.any(
      requests.map(async (pending) => {
        const response = await pending;
        if (response.status() !== 503) throw new Error("Admitted sample");
        return response;
      }),
    );
    expect(await firstBusy.json()).toMatchObject({
      detail: { code: "preview-busy" },
    });
    const matchesReport = (address: string) => {
      const url = new URL(address);
      return (
        url.pathname === base + "busy.report.json" &&
        url.searchParams.get("preview") === "hartmesh.legacy-report/report"
      );
    };
    const [browserBusy] = await Promise.all([
      page.waitForResponse((response) => matchesReport(response.url()), {
        timeout: 10_000,
      }),
      report.click(),
    ]);
    evidence.browserBusyStatus = browserBusy.status();
    expect(browserBusy.status()).toBe(503);
    expect(await browserBusy.json()).toMatchObject({
      detail: { code: "preview-busy" },
    });
    const busy = page.getByRole("group", {
      name: "Previews are busy. Try again in a moment.",
      exact: true,
    });
    await expect(busy).toBeVisible({ timeout: 5000 });
    await expect(
      busy.getByRole("button", { name: "Retry", exact: true }),
    ).toBeVisible();
    await expect(
      busy.getByRole("link", { name: "Download", exact: true }),
    ).toBeVisible();
    const ready = await request.get("/health/ready", { timeout: 5000 });
    burst.readiness = ready.status();
    expect(ready.status()).toBe(200);
    for (let i = 0; i < trials; i++) {
      // Begin within the first second of a finite 2.5-second callback, so the
      // same PID can bracket the HTTP read. Sampling is outside the timer.
      let beforePid = 0;
      await expect
        .poll(
          () => {
            const workers = runningWorkers();
            expect(workers.length).toBeLessThanOrEqual(1);
            const [worker] = workers;
            if (worker?.pid && worker.ageSeconds === 0) beforePid = worker.pid;
            return beforePid;
          },
          { intervals: [35], timeout: 3500 },
        )
        .toBeGreaterThan(0);
      during.push(await list("list-during-concurrent-previews"));
      const afterPids = runningWorkers().map((worker) => worker.pid);
      overlaps.push({ beforePid, afterPids });
      persist();
      expect(afterPids).toEqual([beforePid]);
    }
    const results = await Promise.all(requests);
    for (const [index, response] of results.entries()) {
      expect([200, 503]).toContain(response.status());
      if (response.status() === 503) {
        expect(await response.json()).toMatchObject({
          detail: { code: "preview-busy" },
        });
        expect(response.headers()["retry-after"]).toMatch(/^[1-9]\d*$/);
      } else {
        expect(response.headers()["x-artifact-projection"]).toBe("sample-v1");
        expect(await response.text()).toBe("sample-" + index);
      }
    }
    expect(statuses).toContain(200);
    expect(statuses).toContain(503);
    evidence.listingMedianRatio = median(during) / median(listing);
    expect.soft(median(during)).toBeLessThanOrEqual(median(listing) * 1.1);
    expect(runningWorkers()).toHaveLength(0);
    const recoveryAttempts: { status: number; elapsedMs: number }[] = [];
    evidence.recoveryAttempts = recoveryAttempts;
    const recoveryStarted = performance.now();
    const recoveryDeadline = recoveryStarted + 12_000;
    // A distinct uncached sample must complete before the user's Retry.
    while (true) {
      const remaining = Math.ceil(recoveryDeadline - performance.now());
      expect(
        remaining,
        "worker did not recover within 12 seconds",
      ).toBeGreaterThan(0);
      const recovered = await sample("recovery.sample", remaining);
      recoveryAttempts.push({
        status: recovered.status(),
        elapsedMs: performance.now() - recoveryStarted,
      });
      persist();
      if (recovered.status() === 200) {
        expect(recovered.headers()["x-artifact-projection"]).toBe("sample-v1");
        expect(await recovered.text()).toBe("recovered");
        break;
      }
      expect(recovered.status()).toBe(503);
      expect(await recovered.json()).toMatchObject({
        detail: { code: "preview-busy" },
      });
      const retry = recovered.headers()["retry-after"];
      expect(retry).toMatch(/^[1-9]\d*$/);
      await new Promise((resolve) =>
        setTimeout(
          resolve,
          Math.min(
            Number(retry) * 1000,
            Math.max(0, recoveryDeadline - performance.now()),
          ),
        ),
      );
    }
    const [retriedResponse] = await Promise.all([
      page.waitForResponse((response) => matchesReport(response.url()), {
        timeout: 10_000,
      }),
      busy.getByRole("button", { name: "Retry", exact: true }).click(),
    ]);
    evidence.browserRetryStatus = retriedResponse.status();
    expect(retriedResponse.status()).toBe(200);
    expect(retriedResponse.headers()["x-artifact-projection"]).toBe(
      "business-report-v1",
    );
    await expect(
      page.getByTestId("plugin-artifact-presentation"),
    ).toContainText("August 2026 Business Review");
    const after = memoryPeak();
    evidence.peakAfterPreviewsBytes = after;
    expect.soft(after - peak).toBeLessThanOrEqual(288 * mebibyte);

    // Let the independent one-minute admission budget expire so the full
    // per-invocation deadline is available to this final request.
    const deadlineEvidence: Record<string, unknown> = {
      finiteSleepSeconds: 11,
      workerDeadlineSeconds: 10,
      quietWindowMs,
    };
    evidence.fullDeadline = deadlineEvidence;
    persist();
    await timed(
      "quiet-before-deadline",
      () => new Promise((resolve) => setTimeout(resolve, quietWindowMs)),
    );
    const elapsed = await timed("full-worker-deadline", async () => {
      const [response, readiness] = await Promise.all([
        sample("deadline.sample"),
        (async () => {
          await expect
            .poll(() => runningWorkers().length, {
              intervals: [25],
              timeout: 3000,
            })
            .toBe(1);
          return request.get("/health/ready", { timeout: 5000 });
        })(),
      ]);
      deadlineEvidence.readiness = readiness.status();
      deadlineEvidence.status = response.status();
      expect(readiness.status()).toBe(200);
      expect(response.status()).toBe(503);
      expect(await response.text()).toContain("preview-busy");
    });
    const deadlineDigest = sha256(Buffer.from("deadline"));
    const audit = gatewayLogs().find((line) =>
      line.includes("sha256=" + deadlineDigest + " reason=wall-deadline"),
    );
    deadlineEvidence.sourceSha256 = deadlineDigest;
    deadlineEvidence.wallDeadlineLogged = Boolean(audit);
    deadlineEvidence.httpElapsedMs = elapsed;
    expect(audit).toBeDefined();
    expect(elapsed).toBeGreaterThanOrEqual(9900);
    expect(elapsed).toBeLessThan(12_000);
    expect(gatewayIdentity()).toBe(original);
    expect(runningWorkers()).toHaveLength(0);
  } finally {
    persist();
  }
});
