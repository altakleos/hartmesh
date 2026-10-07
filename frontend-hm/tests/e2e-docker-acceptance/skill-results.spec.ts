import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { expect, test, type BrowserContext } from "@playwright/test";

const password = "synthetic-acceptance-password-123";
const packages = ["supplier-comparison", "procedure-summary"] as const;

async function csrf(context: BrowserContext) {
  const cookie = (await context.cookies()).find(
    (item) => item.name === "csrf_token",
  );
  if (!cookie) throw new Error("Real authenticated CSRF cookie missing");
  return { "X-CSRF-Token": cookie.value };
}

async function createOwner(context: BrowserContext, email: string) {
  const initialized = await context.request.post("/api/v1/auth/initialize", {
    data: { email: "admin@example.com", password },
  });
  expect([201, 409]).toContain(initialized.status());
  if (initialized.status() === 409)
    expect(
      ((await initialized.json()) as { detail: { code: string } }).detail.code,
    ).toBe("system_already_initialized");
  await context.clearCookies();
  const registered = await context.request.post("/api/v1/auth/register", {
    data: { email, password },
  });
  expect(registered.status(), await registered.text()).toBe(201);
  await context.clearCookies();
  const login = await context.request.post("/api/v1/auth/login/local", {
    form: { username: email, password },
  });
  expect(login.status(), await login.text()).toBe(200);
  const identity = await context.request.get("/api/v1/auth/me");
  expect(((await identity.json()) as { system_role: string }).system_role).toBe(
    "user",
  );
}

function archiveFor(packageName: string) {
  const python = process.env.HARTMESH_ACCEPTANCE_PYTHON;
  if (!python) throw new Error("Run through scripts/docker_acceptance.py");
  const root = fileURLToPath(
    new URL(`../../../examples/skills/${packageName}/`, import.meta.url),
  );
  return execFileSync(
    python,
    [
      "-B",
      "-c",
      `import io, pathlib, sys, zipfile
root=pathlib.Path(sys.argv[1]); output=io.BytesIO()
with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
    for file in sorted(root.rglob("*")):
        if file.is_symlink(): raise ValueError("linked fixture")
        if file.is_file() and "__pycache__" not in file.parts:
            archive.write(file, pathlib.Path(root.name) / file.relative_to(root))
sys.stdout.buffer.write(output.getvalue())`,
      root,
    ],
    { timeout: 10_000 },
  );
}

const sha = (buffer: Buffer) =>
  createHash("sha256").update(buffer).digest("hex");

test("ordinary skill archives produce rich views through real owner APIs and tools", async ({
  page,
  context,
  browser,
}) => {
  await createOwner(context, "skill-owner@example.com");
  const before = await context.request.get("/api/skills");
  expect(((await before.json()) as { skills: unknown[] }).skills).toEqual([]);
  const effective = await context.request.get("/api/features");
  const permissions = (
    (await effective.json()) as {
      customer_administration: {
        local_skill_management: boolean;
        plugin_management: boolean;
        local_mcp_management: boolean;
      };
    }
  ).customer_administration;
  expect(permissions.local_skill_management).toBe(true);
  expect(permissions.plugin_management).toBe(false);
  expect(permissions.local_mcp_management).toBe(false);
  const receipts: Record<string, unknown>[] = [];
  const threads: string[] = [];
  let savedArchive: Buffer | undefined;
  let firstArtifactUrl = "";

  for (const packageName of packages) {
    const archive = archiveFor(packageName);
    const installed = await context.request.post("/api/skills/install/upload", {
      headers: await csrf(context),
      multipart: {
        archive: {
          name: `${packageName}.skill`,
          mimeType: "application/zip",
          buffer: archive,
        },
      },
    });
    expect(installed.status(), await installed.text()).toBe(200);
    const catalog = await context.request.get("/api/skills");
    expect(
      (
        (await catalog.json()) as {
          skills: { name: string; category: string }[];
        }
      ).skills,
    ).toContainEqual(
      expect.objectContaining({ name: packageName, category: "custom" }),
    );
    const created = await context.request.post("/api/langgraph/threads", {
      headers: await csrf(context),
      data: { metadata: {} },
    });
    expect(created.ok(), await created.text()).toBe(true);
    const thread = ((await created.json()) as { thread_id: string }).thread_id;
    threads.push(thread);
    const filename =
      packageName === "supplier-comparison" ? "quotes.json" : "procedure.json";
    const input = readFileSync(
      new URL(`../../../examples/skills/inputs/${filename}`, import.meta.url),
    );
    const uploaded = await context.request.post(
      `/api/threads/${thread}/uploads`,
      {
        headers: await csrf(context),
        multipart: {
          files: {
            name: filename,
            mimeType: "application/json",
            buffer: input,
          },
        },
      },
    );
    expect(uploaded.ok(), await uploaded.text()).toBe(true);
    const ran = await context.request.post(
      `/api/langgraph/threads/${thread}/runs/wait`,
      {
        headers: await csrf(context),
        data: {
          assistant_id: "lead_agent",
          input: {
            messages: [
              { role: "user", content: `acceptance:skill:${packageName}` },
            ],
          },
        },
      },
    );
    expect(ran.ok(), await ran.text()).toBe(true);
    const stateResponse = await context.request.get(
      `/api/langgraph/threads/${thread}/state`,
    );
    expect(stateResponse.ok(), await stateResponse.text()).toBe(true);
    const state = (await stateResponse.json()) as {
      values: {
        artifacts: string[];
        messages: {
          type?: string;
          role?: string;
          tool_call_id?: string;
          name?: string;
          data?: {
            type?: string;
            role?: string;
            tool_call_id?: string;
            name?: string;
          };
        }[];
      };
    };
    const toolNames = state.values.messages
      .map((message) => message.data ?? message)
      .filter(
        (message) =>
          message.type === "tool" ||
          message.role === "tool" ||
          message.tool_call_id,
      )
      .map((message) => message.name);
    expect(toolNames).toEqual([
      "read_file",
      "read_file",
      "bash",
      "present_files",
    ]);
    const paths = state.values.artifacts;
    expect(paths).toHaveLength(4);
    const viewPath = paths.find((path) => path.endsWith(".view.json"))!;
    const base = viewPath.slice(0, viewPath.lastIndexOf("/") + 1);
    const stem =
      packageName === "supplier-comparison" ? "comparison" : "procedure";
    const viewUrl = `/api/threads/${thread}/artifacts${viewPath}`;
    const viewResponse = await context.request.get(viewUrl);
    expect(viewResponse.ok(), await viewResponse.text()).toBe(true);
    const viewBytes = await viewResponse.body();
    const view = JSON.parse(viewBytes.toString("utf-8")) as {
      title: string;
      primary_source: { path: string };
      exports: { path: string; label: string }[];
    };
    expect(view.exports).toHaveLength(2);
    const sourceResponse = await context.request.get(
      `/api/threads/${thread}/artifacts${base}${view.primary_source.path}`,
    );
    expect(await sourceResponse.body()).toEqual(input);
    const hashes: Record<string, string> = {
      view: sha(viewBytes),
      source: sha(input),
      archive: sha(archive),
    };
    for (const item of view.exports) {
      const exported = await context.request.get(
        `/api/threads/${thread}/artifacts${base}${item.path}`,
      );
      expect(exported.ok(), await exported.text()).toBe(true);
      const bytes = await exported.body();
      hashes[item.path] = sha(bytes);
      if (item.path.endsWith(".pdf"))
        expect(bytes.subarray(0, 5).toString()).toBe("%PDF-");
      else expect(bytes.subarray(0, 2).toString()).toBe("PK");
    }
    receipts.push({
      package: packageName,
      thread,
      tools: toolNames,
      files: paths,
      hashes,
    });
    await page.goto(`/workspace/chats/${thread}`);
    await page.getByTestId("artifact-trigger").click();
    await page
      .getByRole("complementary")
      .getByText(`${stem}.view.json`, { exact: true })
      .click();
    const card = page.getByTestId("artifact-view");
    await expect(
      card.getByRole("heading", { name: view.title, exact: true }),
    ).toBeVisible();
    await expect(card).toHaveCount(1);
    await expect(page.getByTestId("plugin-artifact-presentation")).toHaveCount(
      0,
    );
    for (const item of view.exports) {
      await expect(
        card.getByRole("link", { name: `Download ${item.label}`, exact: true }),
      ).toBeVisible();
      await expect(
        card.getByRole("checkbox", { name: item.label, exact: true }),
      ).toBeChecked();
      const downloading = page.waitForEvent("download");
      await card
        .getByRole("link", { name: `Download ${item.label}`, exact: true })
        .click();
      const download = await downloading;
      expect(download.suggestedFilename()).toBe(item.path);
      const downloadedPath = await download.path();
      expect(downloadedPath).not.toBeNull();
      expect(sha(readFileSync(downloadedPath))).toBe(hashes[item.path]);
    }
    if (packageName === "supplier-comparison") {
      await expect(card.getByRole("table")).toBeVisible();
      await expect(card.getByText("25.03", { exact: true })).toBeVisible();
      await expect(card.getByText("24.98", { exact: true })).toBeVisible();
      await expect(
        card.getByText("Supplier A: Shipping excluded", { exact: true }),
      ).toBeVisible();
      await card
        .getByRole("checkbox", { name: "Quoted terms workbook", exact: true })
        .uncheck();
      const saved = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" &&
          new URL(response.url()).pathname === `/api/threads/${thread}/files`,
      );
      await card
        .getByRole("button", { name: "Save to My files", exact: true })
        .click();
      expect((await saved).ok()).toBe(true);
      const myFiles = await context.request.get("/api/files");
      expect(await myFiles.text()).toContain("Comparisons/comparison.pdf");
      expect(await myFiles.text()).not.toContain("comparison.xlsx");
      const shared = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" &&
          new URL(response.url()).pathname === "/api/shared/publish",
      );
      await card
        .getByRole("button", { name: "Share with everyone", exact: true })
        .click();
      expect((await shared).ok()).toBe(true);
      await expect
        .poll(async () =>
          (await (await context.request.get("/api/shared")).text()).includes(
            "Comparisons/comparison.pdf",
          ),
        )
        .toBe(true);
      await page.getByRole("button", { name: "Undo", exact: true }).click();
      await expect
        .poll(async () =>
          (await (await context.request.get("/api/shared")).text()).includes(
            "Comparisons/comparison.pdf",
          ),
        )
        .toBe(false);
      const manifest = await context.request.get(
        `/api/skills/custom/${packageName}/export-manifest`,
      );
      expect(manifest.ok(), await manifest.text()).toBe(true);
      const revision = ((await manifest.json()) as { revision: string })
        .revision;
      const exported = await context.request.get(
        `/api/skills/custom/${packageName}/export?expected_revision=${revision}`,
      );
      expect(exported.ok(), await exported.text()).toBe(true);
      savedArchive = await exported.body();
      firstArtifactUrl = viewUrl;
    } else {
      await expect(
        card.getByText("Inspect the work area.", { exact: true }),
      ).toBeVisible();
      await expect(
        card.getByText("Inspection interval not supplied", { exact: true }),
      ).toBeVisible();
      await expect(
        card.getByText("Consult the original procedure", { exact: false }),
      ).toBeVisible();
      const saved = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" &&
          new URL(response.url()).pathname === `/api/threads/${thread}/files`,
      );
      await card
        .getByRole("button", { name: "Save to My files", exact: true })
        .click();
      expect((await saved).ok()).toBe(true);
      await expect
        .poll(async () => {
          const files = await (await context.request.get("/api/files")).text();
          return (
            files.includes("procedure.pdf") && files.includes("procedure.docx")
          );
        })
        .toBe(true);
      const shared = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" &&
          new URL(response.url()).pathname === "/api/shared/publish",
      );
      await card
        .getByRole("button", { name: "Share with everyone", exact: true })
        .click();
      expect((await shared).ok()).toBe(true);
      await expect
        .poll(async () => {
          const files = await (await context.request.get("/api/shared")).text();
          return (
            files.includes("procedure.pdf") && files.includes("procedure.docx")
          );
        })
        .toBe(true);
      await page.getByRole("button", { name: "Undo", exact: true }).click();
      await expect
        .poll(async () => {
          const files = await (await context.request.get("/api/shared")).text();
          return (
            files.includes("procedure.pdf") || files.includes("procedure.docx")
          );
        })
        .toBe(false);
    }
    await page.screenshot({
      path: `${test.info().outputDir}/${stem}.png`,
      fullPage: true,
    });
    await page.setViewportSize({ width: 390, height: 844 });
    await page.emulateMedia({ colorScheme: "dark" });
    await expect(
      card.getByRole("heading", { name: view.title, exact: true }),
    ).toBeVisible();
    await expect
      .poll(async () =>
        page.evaluate(() =>
          document.documentElement.classList.contains("dark"),
        ),
      )
      .toBe(true);
    await expect
      .poll(async () =>
        page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      )
      .toBe(true);
    // Wait for the mobile drawer to finish entering before certifying its layout.
    // overflow:hidden can conceal an off-screen card from document-width checks.
    await expect
      .poll(async () =>
        card.evaluate((element) => {
          const bounds = element.getBoundingClientRect();
          return bounds.left >= 0 && bounds.right <= window.innerWidth;
        }),
      )
      .toBe(true);
    await page.screenshot({
      path: `${test.info().outputDir}/${stem}-mobile-dark.png`,
      fullPage: true,
    });
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.emulateMedia({ colorScheme: "light" });
  }

  const fallbackCreated = await context.request.post("/api/langgraph/threads", {
    headers: await csrf(context),
    data: { metadata: {} },
  });
  expect(fallbackCreated.ok()).toBe(true);
  const fallbackThread = (
    (await fallbackCreated.json()) as { thread_id: string }
  ).thread_id;
  const fallbackRun = await context.request.post(
    `/api/langgraph/threads/${fallbackThread}/runs/wait`,
    {
      headers: await csrf(context),
      data: {
        assistant_id: "lead_agent",
        input: { messages: [{ role: "user", content: "acceptance:fallback" }] },
      },
    },
  );
  expect(fallbackRun.ok(), await fallbackRun.text()).toBe(true);
  const ordinaryPdf = await context.request.get(
    `/api/threads/${fallbackThread}/artifacts/mnt/user-data/outputs/plain.pdf`,
  );
  expect(ordinaryPdf.ok(), await ordinaryPdf.text()).toBe(true);
  expect((await ordinaryPdf.body()).subarray(0, 5).toString()).toBe("%PDF-");
  const invalidView = await context.request.get(
    `/api/threads/${fallbackThread}/artifacts/mnt/user-data/outputs/broken.view.json`,
  );
  expect(invalidView.ok(), await invalidView.text()).toBe(true);
  expect(((await invalidView.json()) as { version: number }).version).toBe(999);
  await page.goto(`/workspace/chats/${fallbackThread}`);
  await page.getByTestId("artifact-trigger").click();
  await page
    .getByRole("complementary")
    .getByText("broken.view.json", { exact: true })
    .click();
  await expect(
    page.getByText("Ordinary fallback files ready.", { exact: true }),
  ).toBeVisible();
  await expect(page.getByTestId("artifact-view")).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Download", exact: true }).first(),
  ).toBeVisible();
  await page
    .getByRole("combobox")
    .filter({ hasText: "broken.view.json" })
    .click();
  await page.getByRole("option", { name: "plain.pdf", exact: true }).click();
  const plainDownloading = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download", exact: true }).click();
  const plainDownload = await plainDownloading;
  const plainDownloadedPath = await plainDownload.path();
  expect(plainDownloadedPath).not.toBeNull();
  expect(sha(readFileSync(plainDownloadedPath))).toBe(
    sha(await ordinaryPdf.body()),
  );
  const ordinarySaved = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      new URL(response.url()).pathname ===
        `/api/threads/${fallbackThread}/files`,
  );
  await page
    .getByRole("button", { name: "Save to My files", exact: true })
    .click();
  expect((await ordinarySaved).ok()).toBe(true);
  expect(await (await context.request.get("/api/files")).text()).toContain(
    "plain.pdf",
  );
  const ordinaryShared = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      new URL(response.url()).pathname === "/api/shared/publish",
  );
  await page
    .getByRole("button", { name: "Share with everyone", exact: true })
    .click();
  expect((await ordinaryShared).ok()).toBe(true);
  await expect
    .poll(async () =>
      (await (await context.request.get("/api/shared")).text()).includes(
        '"path":"plain.pdf"',
      ),
    )
    .toBe(true);
  await page.getByRole("button", { name: "Undo", exact: true }).click();
  await expect
    .poll(async () =>
      (await (await context.request.get("/api/shared")).text()).includes(
        '"path":"plain.pdf"',
      ),
    )
    .toBe(false);
  receipts.push({
    fallback_thread: fallbackThread,
    plain_pdf_sha256: sha(await ordinaryPdf.body()),
    malformed_view_sha256: sha(await invalidView.body()),
  });

  const recipient = await browser.newContext({
    baseURL: process.env.HARTMESH_ACCEPTANCE_URL,
  });
  try {
    await createOwner(recipient, "skill-recipient@example.com");
    expect(
      (
        (await (await recipient.request.get("/api/skills")).json()) as {
          skills: unknown[];
        }
      ).skills,
    ).toEqual([]);
    expect([403, 404]).toContain(
      (await recipient.request.get(firstArtifactUrl)).status(),
    );
    expect(
      (
        await recipient.request.get(
          "/api/skills/custom/supplier-comparison/history",
        )
      ).status(),
    ).toBe(404);
    const emptyFiles = await recipient.request.get("/api/files");
    expect(await emptyFiles.text()).not.toContain("comparison.pdf");
    expect(savedArchive).toBeDefined();
    const ownerThread = threads[0]!;
    const sharedUpload = await context.request.post(
      `/api/threads/${ownerThread}/uploads`,
      {
        headers: await csrf(context),
        multipart: {
          files: {
            name: "supplier-comparison.skill",
            mimeType: "application/zip",
            buffer: savedArchive!,
          },
        },
      },
    );
    expect(sharedUpload.ok(), await sharedUpload.text()).toBe(true);
    const sharedPackage = await context.request.post("/api/shared/publish", {
      headers: await csrf(context),
      data: {
        thread_id: ownerThread,
        path: "/mnt/user-data/uploads/supplier-comparison.skill",
      },
    });
    expect(sharedPackage.ok(), await sharedPackage.text()).toBe(true);
    const sharedBytes = await recipient.request.get(
      "/api/shared/supplier-comparison.skill",
    );
    expect(sharedBytes.ok(), await sharedBytes.text()).toBe(true);
    expect(await sharedBytes.body()).toEqual(savedArchive!);
    expect(
      (
        (await (await recipient.request.get("/api/skills")).json()) as {
          skills: unknown[];
        }
      ).skills,
    ).toEqual([]);
    const adopted = await recipient.request.post("/api/skills/install/upload", {
      headers: await csrf(recipient),
      multipart: {
        archive: {
          name: "supplier-comparison.skill",
          mimeType: "application/zip",
          buffer: await sharedBytes.body(),
        },
      },
    });
    expect(adopted.ok(), await adopted.text()).toBe(true);
    expect(
      (
        (await (await recipient.request.get("/api/skills")).json()) as {
          skills: { name: string }[];
        }
      ).skills,
    ).toContainEqual(expect.objectContaining({ name: "supplier-comparison" }));
  } finally {
    await recipient.close();
  }
  writeFileSync(
    `${test.info().outputDir}/skill-results.json`,
    JSON.stringify(receipts, null, 2),
  );
});
