import { expect, test, type Page } from "@playwright/test";

import { mockLangGraphAPI } from "./utils/mock-api";

const THREAD = "00000000-0000-0000-0000-000000004200";
const DIR = "/mnt/user-data/outputs/summary";
const VIEW = `${DIR}/summary.view.json`;
const PDF = `${DIR}/summary.pdf`;
const WORD = `${DIR}/summary.docx`;
const SOURCE = `${DIR}/source.json`;
const PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==",
  "base64",
);
const fixture = {
  format: "hartmesh.artifact-view",
  version: 1,
  title: "Document summary",
  subtitle: "Prepared from supplied files",
  accent: "#00ff00",
  primary_source: { path: "source.json" },
  destination: { collection: "Summaries" },
  blocks: [
    {
      type: "facts",
      items: [
        { label: "Total", value: "$9,999,999.99" },
        { label: "Count", value: "178" },
      ],
    },
    {
      type: "text",
      heading: "Scope",
      paragraphs: ["<script>literal source text</script>"],
    },
    {
      type: "list",
      heading: "Next steps",
      ordered: true,
      items: ["Review the source", "Keep the selected files"],
    },
    {
      type: "table",
      heading: "Items",
      columns: [{ label: "Name" }, { label: "Value", align: "end" }],
      rows: [["Item A", "$10.00"]],
      footer: ["Total", "$10.00"],
    },
    {
      type: "image",
      path: "pixel.png",
      alt: "Local sample image",
      caption: "Generated locally",
    },
    {
      type: "notice",
      tone: "warning",
      text: "Review the supplied documents before acting.",
      attribution: "Document summary skill",
    },
  ],
  exports: [
    { path: "summary.pdf", label: "Printable summary" },
    { path: "summary.docx", label: "Editable summary" },
    { path: "unpresented.xlsx", label: "Unpresented export" },
  ],
};

async function openView(
  page: Page,
  options: {
    body?: string;
    image?: Buffer | string;
    available?: () => boolean;
    saved?: Record<string, unknown>[];
    shared?: Record<string, unknown>[];
    read?: () => void;
    omitRevision?: boolean;
  } = {},
) {
  const presented = [VIEW, PDF, WORD, SOURCE, `${DIR}/pixel.png`];
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: THREAD,
        title: "Summary",
        artifacts: [...presented, `${DIR}/unpresented.xlsx`],
        messages: [
          {
            type: "human",
            id: "human-summary",
            content: "Summarize the supplied files.",
          },
          {
            type: "ai",
            id: "ai-summary",
            content: "Here are the files.",
            tool_calls: [
              {
                id: "present-summary",
                name: "present_files",
                args: { filepaths: presented },
              },
            ],
          },
          {
            type: "tool",
            id: "tool-summary",
            tool_call_id: "present-summary",
            name: "present_files",
            content: "Files presented.",
            additional_kwargs: { presented_files: presented },
          },
        ],
      },
    ],
  });
  await page.route(`**/api/threads/${THREAD}/artifacts${VIEW}*`, (route) => {
    options.read?.();
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      headers: options.omitRevision ? {} : { ETag: `"${"a".repeat(64)}"` },
      body: options.body ?? JSON.stringify(fixture),
    });
  });
  await page.route(
    `**/api/threads/${THREAD}/artifacts${DIR}/pixel.png*`,
    (route) =>
      route.fulfill({
        status: 200,
        contentType: "image/png",
        body: options.image ?? PNG,
      }),
  );
  for (const path of [PDF, WORD])
    await page.route(`**/api/threads/${THREAD}/artifacts${path}*`, (route) =>
      route.fulfill({
        status: options.available?.() === false ? 503 : 206,
        headers: { "Content-Range": "bytes 0-0/100" },
        body: "x",
      }),
    );
  await page.route(`**/api/threads/${THREAD}/files`, (route) => {
    const request = route.request().postDataJSON() as {
      path: string;
      folder?: string;
      thread_id?: string;
    };
    options.saved?.push(request);
    const name = String(request.path).split("/").at(-1)!;
    const path = request.folder ? `${request.folder}/${name}` : name;
    return route.fulfill({
      json: {
        path,
        name,
        size: 100,
        modified: 1,
        virtual_path: `/mnt/user-data/files/${path}`,
        url: `/api/files/${path}`,
      },
    });
  });
  await page.route("**/api/shared/publish", (route) => {
    const request = route.request().postDataJSON() as {
      path: string;
      folder?: string;
      thread_id?: string;
    };
    options.shared?.push(request);
    const name = String(request.path).split("/").at(-1)!;
    const path = request.folder ? `${request.folder}/${name}` : name;
    return route.fulfill({
      status: 201,
      json: {
        path,
        name,
        size: 100,
        modified: 1,
        virtual_path: `/mnt/user-data/shared/${path}`,
        url: `/api/shared/${path}`,
        can_remove: true,
        published_by: "Example user",
        published_at: "2026-01-01T00:00:00Z",
        from_thread_id: THREAD,
        publication_id: "00000000-0000-0000-0000-000000004201",
      },
    });
  });
  await page.goto(`/workspace/chats/${THREAD}`);
  await expect(page.getByText("summary.view.json").first()).toBeVisible({
    timeout: 15_000,
  });
  await page.getByText("summary.view.json").first().click();
  return page.getByTestId("artifact-view");
}

test("renders all passive primitives and offers only recorded exports", async ({
  page,
}) => {
  const card = await openView(page);
  await expect(
    card.getByRole("heading", { name: "Document summary" }),
  ).toBeVisible();
  await expect(
    card.getByText("<script>literal source text</script>"),
  ).toBeVisible();
  await expect(card.locator("script")).toHaveCount(0);
  await expect(card.getByRole("table")).toBeVisible();
  await expect(card.getByAltText("Local sample image")).toHaveJSProperty(
    "naturalWidth",
    1,
  );
  await expect(
    card.getByRole("link", { name: "Download Printable summary" }),
  ).toBeVisible();
  await expect(card.getByText("Unpresented export")).toHaveCount(0);
});

test("keeps ordinary access when insecure hashing and the Gateway provide no strong revision", async ({
  page,
}) => {
  await page.addInitScript(() => {
    Object.defineProperty(globalThis.crypto, "subtle", {
      value: undefined,
      configurable: true,
    });
  });
  await openView(page, { omitRevision: true });
  await expect(page.getByTestId("artifact-view")).toHaveCount(0);
  await expect(
    page.getByText(
      "This file is not a supported result view. The original file remains available.",
    ),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Download", exact: true }).first(),
  ).toBeVisible();
});

test("selected file actions keep their explicit destination and publication Undo", async ({
  page,
}) => {
  const saved: Record<string, unknown>[] = [];
  const shared: Record<string, unknown>[] = [];
  const card = await openView(page, { saved, shared });
  await expect(
    card.getByRole("checkbox", { name: "Editable summary" }),
  ).toBeVisible();
  await card.getByRole("checkbox", { name: "Editable summary" }).uncheck();
  await card.getByRole("button", { name: "Save to My files" }).click();
  await expect.poll(() => saved).toEqual([{ path: PDF, folder: "Summaries" }]);
  await card.getByRole("button", { name: "Share with everyone" }).click();
  await expect
    .poll(() => shared)
    .toEqual([{ path: PDF, thread_id: THREAD, folder: "Summaries" }]);
  await expect(
    page.getByRole("button", { name: "Undo", exact: true }),
  ).toBeVisible();
});

for (const theme of ["light", "dark"])
  test(`keeps text readable and tables keyboard accessible at 360px in ${theme}`, async ({
    page,
  }) => {
    await page.setViewportSize({ width: 360, height: 800 });
    await page.addInitScript(
      (value) => localStorage.setItem("theme", value),
      theme,
    );
    const card = await openView(page);
    await expect(card).toBeVisible();
    const measurements = await card.evaluate((element) => {
      const title = element.querySelector("h2")!;
      const value = element.querySelector("dd")!;
      const range = document.createRange();
      range.selectNodeContents(value);
      return {
        overflow: element.scrollWidth - element.clientWidth,
        color: getComputedStyle(title).color,
        lines: range.getClientRects().length,
      };
    });
    expect(measurements.overflow).toBeLessThanOrEqual(1);
    expect(measurements.color).not.toBe("rgb(0, 255, 0)");
    expect(measurements.lines).toBe(1);
    const table = card.getByRole("region", { name: "Items" });
    await table.focus();
    await expect(table).toBeFocused();
  });

test("ignores an unsafe collection visibly and uses the ordinary destination", async ({
  page,
}) => {
  const saved: Record<string, unknown>[] = [];
  const card = await openView(page, {
    saved,
    body: JSON.stringify({
      ...fixture,
      destination: { collection: "../Private" },
    }),
  });
  await expect(
    card.getByText(
      "The suggested folder is unavailable. Files will use the default destination.",
    ),
  ).toBeVisible();
  await expect(
    card.getByRole("button", { name: "Save to My files" }),
  ).toBeEnabled();
  await card.getByRole("button", { name: "Save to My files" }).click();
  await expect.poll(() => saved.length).toBe(2);
  expect(saved.every((request) => request.folder === undefined)).toBe(true);
});

test("contains an invalid image failure without withholding downloads", async ({
  page,
}) => {
  const card = await openView(page, { image: "<svg>not a raster</svg>" });
  await expect(
    card.getByText("Image unavailable or outside the display limits."),
  ).toBeVisible();
  await expect(
    card.getByRole("link", { name: "Download Printable summary" }),
  ).toBeVisible();
});

test("retries uncertain export probes without reloading the view", async ({
  page,
}) => {
  let available = false;
  let reads = 0;
  const card = await openView(page, {
    available: () => available,
    read: () => {
      reads += 1;
    },
  });
  await expect(
    card.getByText(
      "Some files could not be checked. Retry to check them again.",
    ),
  ).toBeVisible();
  await expect(
    card.getByRole("link", { name: "Download Printable summary" }),
  ).toHaveCount(0);
  const before = reads;
  available = true;
  await card.getByRole("button", { name: "Retry check" }).click();
  await expect(
    card.getByRole("link", { name: "Download Printable summary" }),
  ).toBeVisible();
  expect(reads).toBe(before);
});

for (const body of [
  JSON.stringify({ ...fixture, version: 2 }),
  " ".repeat(1024 * 1024 + 1) + JSON.stringify(fixture),
])
  test(`keeps original access when structured rendering is refused (${body.startsWith(" ") ? "oversized" : "version"})`, async ({
    page,
  }) => {
    await openView(page, { body });
    await expect(page.getByTestId("artifact-view")).toHaveCount(0);
    await expect(
      page.getByText(
        "This file is not a supported result view. The original file remains available.",
      ),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Download", exact: true }).first(),
    ).toBeVisible();
  });
