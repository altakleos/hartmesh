import { expect, test, type Page } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

const THREAD = "00000000-0000-0000-0000-000000004300";
const SOURCE = "/mnt/user-data/outputs/sample.summary.json";
const PDF = "/mnt/user-data/outputs/sample.pdf";

async function installFixture(
  page: Page,
  options: {
    kind?: "native" | "passive";
    enabled?: boolean;
    broken?: boolean;
    label?: () => string;
    entryRevision?: () => string;
  } = {},
) {
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: THREAD,
        title: "Summary sample",
        artifacts: [SOURCE, PDF],
        messages: [
          { type: "human", id: "human-summary", content: "Show the sample." },
          {
            type: "ai",
            id: "ai-summary",
            content: "Here are the files.",
            tool_calls: [
              {
                id: "present-summary",
                name: "present_files",
                args: { filepaths: [SOURCE, PDF] },
              },
            ],
          },
          {
            type: "tool",
            id: "tool-summary",
            name: "present_files",
            tool_call_id: "present-summary",
            content: "Files presented.",
            additional_kwargs: { presented_files: [SOURCE, PDF] },
          },
        ],
      },
    ],
  });
  await page.route("**/api/plugins", (route) =>
    route.fulfill({
      json: [
        {
          viewer_id: "default",
          namespace: "example.summary",
          module: "summary.v1",
          entry: `/api/plugins/modules/summary.v1/${options.entryRevision?.() ?? "a".repeat(64)}.mjs`,
          transport: "inline-v1",
          title: "Summary",
          description: "",
          settings: { enabled: options.enabled !== false },
          backend_actions: [],
          artifact_presentations: [
            {
              id: "summary",
              suffixes: [".summary.json"],
              source_max_bytes: 4096,
              preview_max_bytes: 1024,
              projection_marker: null,
            },
          ],
        },
      ],
    }),
  );
  await page.route("**/api/plugins/modules/summary.v1/*.mjs", (route) => {
    const label = JSON.stringify(options.label?.() ?? "Installed summary");
    const artifact =
      options.kind === "passive"
        ? `kind:"passive", present(context){ return {format:"hartmesh.artifact-view", version:1, title:${label}, blocks:[{type:"text",paragraphs:[JSON.parse(context.artifact.content).value]}], exports:[{path:"sample.pdf",label:"Printable sample"},{path:"unpresented.pdf",label:"Unpresented sample"}]}; }`
        : `kind:"native", mount(root,context){ ${options.broken ? 'throw new Error("Unavailable display");' : `const title=document.createElement("h2"); title.textContent=${label}; const value=document.createElement("p"); value.textContent=JSON.parse(context.artifact.content).value; root.append(title,value); return {dispose(){root.replaceChildren();},files:{exports:[{path:"sample.pdf",label:"Printable sample"}]}};`} }`;
    return route.fulfill({
      contentType: "text/javascript",
      body: `export default {apiVersion:1,module:"summary.v1",artifactApiVersion:1,artifacts:[{id:"summary",title:"Summary",${artifact}}],surfaces:[{id:"samples",slot:"page",title:"Summary samples",navigation:{label:"Summary samples"},mount(root){root.textContent="Installed page sample";return{dispose(){root.replaceChildren();}};}}]};`,
    });
  });
  await page.route(`**/api/threads/${THREAD}/artifacts${SOURCE}*`, (route) =>
    route.fulfill({
      contentType: "application/json",
      headers: { ETag: `"${"b".repeat(64)}"` },
      body: '{"value":"canonical value"}',
    }),
  );
  await page.route(`**/api/threads/${THREAD}/artifacts${PDF}*`, (route) =>
    route.fulfill({
      status: 206,
      headers: { "Content-Range": "bytes 0-0/100" },
      body: "x",
    }),
  );
  await page.goto(`/workspace/chats/${THREAD}`);
  await page.getByRole("log").getByText("sample.summary.json").first().click();
}

for (const kind of ["native", "passive"] as const) {
  test(`installed ${kind} presentation works in the production artifact panel`, async ({
    page,
  }) => {
    await installFixture(page, { kind });
    await expect(
      page.getByRole("heading", { name: "Installed summary" }),
    ).toBeVisible();
    await expect(
      page.getByText("canonical value", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Download Printable sample" }),
    ).toBeVisible();
    await expect(page.getByText("Unpresented sample")).toHaveCount(0);
    await page.getByRole("link", { name: "Summary samples" }).click();
    await expect(page.getByText("Installed page sample")).toBeVisible();
  });
}

test("an unavailable installed native renderer retains bounded canonical source", async ({
  page,
}) => {
  await installFixture(page, { broken: true });
  await expect(
    page.getByRole("heading", { name: "Installed summary" }),
  ).toHaveCount(0);
  await expect(page.locator(".cm-content")).toContainText("canonical value");
});

test("a disabled module is not loaded and leaves canonical file access", async ({
  page,
}) => {
  const modules: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/plugins/modules/"))
      modules.push(request.url());
  });
  await installFixture(page, { enabled: false });
  await expect(page.locator(".cm-content")).toContainText("canonical value");
  await expect(page.getByRole("link", { name: "Summary samples" })).toHaveCount(
    0,
  );
  expect(modules).toEqual([]);
});

test("a reloaded installed revision replaces the old browser contribution", async ({
  page,
}) => {
  let revision = "a".repeat(64);
  let label = "First summary";
  await installFixture(page, {
    label: () => label,
    entryRevision: () => revision,
  });
  await expect(
    page.getByRole("heading", { name: "First summary" }),
  ).toBeVisible();
  revision = "c".repeat(64);
  label = "Updated summary";
  await page.reload();
  await page.getByRole("log").getByText("sample.summary.json").first().click();
  await expect(
    page.getByRole("heading", { name: "Updated summary" }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "First summary" }),
  ).toHaveCount(0);
});
