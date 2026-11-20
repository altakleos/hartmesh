import { createServer, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { expect, test, type Locator, type Page } from "@playwright/test";

import {
  mockLangGraphAPI,
  handleRunStream,
  MOCK_RUN_ID,
  MOCK_THREAD_ID,
} from "./utils/mock-api";

const sent = "Please summarize the next step";
const short = "One short reply.";
const history = (count: number) =>
  Array.from({ length: count }, (_, index) => ({
    id: `history-${index}`,
    type: index % 2 === 0 ? "human" : "ai",
    content: `Earlier conversation message ${index}`,
  }));

async function heldChat(
  page: Page,
  count: number,
  agent: boolean,
  text = sent,
) {
  const messages = history(count);
  const thread = {
    thread_id: MOCK_THREAD_ID,
    title: "Conversation",
    messages: [...messages],
  };
  mockLangGraphAPI(page, {
    threads: [thread],
    agents: [{ name: "test-agent", description: "A helpful agent" }],
  });
  let response: ServerResponse | undefined;
  const frame = (event: string, data: unknown) =>
    response!.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  const server = createServer((_req, res) => {
    response = res;
    res.writeHead(200, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Expose-Headers": "Content-Location",
      "Content-Location": `/api/threads/${MOCK_THREAD_ID}/runs/${MOCK_RUN_ID}`,
      "Content-Type": "text/event-stream",
    });
    frame("metadata", { run_id: MOCK_RUN_ID, thread_id: MOCK_THREAD_ID });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/runs/stream`;
  await page.route("**/api/langgraph/threads/*/runs/stream", (route) =>
    route.continue({ url }),
  );
  await page.goto(
    `/workspace/${agent ? "agents/test-agent/" : ""}chats/${MOCK_THREAD_ID}`,
    { waitUntil: "domcontentloaded" },
  );
  const composer = page.getByPlaceholder(/how can i assist you/i);
  const row = page
    .getByText(text, { exact: true })
    .locator("xpath=ancestor::*[@data-message-group-index][1]");
  const viewport = page
    .getByTestId("main-message-list")
    .locator(":scope > div")
    .first();
  return {
    composer,
    row,
    viewport,
    async send() {
      await composer.fill(text);
      await composer.press("Enter");
      await expect.poll(() => Boolean(response)).toBe(true);
    },
    reply(content: string) {
      thread.messages.splice(
        messages.length,
        thread.messages.length,
        { id: "server-human", type: "human", content: text },
        { id: "server-assistant", type: "ai", content },
      );
      frame("values", { messages: thread.messages });
    },
    finish() {
      frame("end", {});
      response!.end();
    },
    async close() {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

async function offset(row: Locator, viewport: Locator) {
  const [rowBox, viewportBox] = await Promise.all([
    row.boundingBox(),
    viewport.boundingBox(),
  ]);
  return rowBox && viewportBox ? Math.round(rowBox.y - viewportBox.y) : -10000;
}

for (const [width, height, count, agent] of [
  [390, 844, 10, false],
  [360, 640, 80, true],
] as const) {
  test(`sent row stays in view through reply and resize (${width}px, ${count} messages)`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height });
    const chat = await heldChat(page, count, agent);
    try {
      await chat.send();
      const aligned = () =>
        expect
          .poll(async () =>
            Math.abs((await offset(chat.row, chat.viewport)) - 16),
          )
          .toBeLessThanOrEqual(2);
      await aligned();
      chat.reply(short);
      await expect(page.getByText(short)).toBeVisible();
      await aligned();
      await page.setViewportSize({ width, height: height - 120 });
      await aligned();
      await page.setViewportSize({ width, height });
      await aligned();
      chat.finish();
      await expect(
        page
          .getByText(short)
          .locator("xpath=ancestor::*[@data-message-group-index][1]")
          .getByRole("button", { name: "Regenerate" }),
      ).toBeEnabled();
      await expect(chat.composer).toBeInViewport({ ratio: 1 });
      await aligned();
      await expect(
        page.getByRole("button", { name: /scroll to bottom/i }),
      ).toBeHidden();
    } finally {
      await chat.close();
    }
  });
}

test("a long answer grows below the sent row and the jump reaches the real bottom", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const chat = await heldChat(page, 16, false);
  try {
    await chat.send();
    await expect.poll(() => offset(chat.row, chat.viewport)).toBe(16);
    const long = Array.from(
      { length: 35 },
      (_, i) => `Paragraph ${i + 1} about the conversation.`,
    ).join("\n\n");
    chat.reply(long);
    const jump = page.getByRole("button", { name: /scroll to bottom/i });
    await expect(jump).toBeVisible();
    await expect.poll(() => offset(chat.row, chat.viewport)).toBe(16);
    await jump.click();
    await expect(
      page.getByText("Paragraph 35 about the conversation."),
    ).toBeInViewport();
    await expect
      .poll(() =>
        chat.viewport.evaluate(
          (v) => v.scrollHeight - v.clientHeight - v.scrollTop,
        ),
      )
      .toBeLessThanOrEqual(2);
  } finally {
    await chat.close();
  }
});

test("a message taller than the viewport keeps the ordinary bottom-follow behavior", async ({
  page,
}) => {
  await page.setViewportSize({ width: 360, height: 640 });
  const text = "Sentence with enough words to wrap on a phone. "
    .repeat(80)
    .trim();
  const chat = await heldChat(page, 8, false, text);
  try {
    await chat.send();
    await expect
      .poll(() => chat.row.evaluate((r) => r.clientHeight))
      .toBeGreaterThan(await chat.viewport.evaluate((v) => v.clientHeight));
    await expect.poll(() => offset(chat.row, chat.viewport)).toBeLessThan(0);
    chat.reply(short);
    await expect(page.getByText(short)).toBeInViewport();
  } finally {
    await chat.close();
  }
});

test("keyboard scrolling releases the held row while the answer is arriving", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const chat = await heldChat(page, 20, false);
  try {
    await chat.send();
    await expect.poll(() => offset(chat.row, chat.viewport)).toBe(16);
    await chat.viewport.evaluate((v) => {
      v.tabIndex = 0;
      v.focus({ preventScroll: true });
    });
    await page.keyboard.press("PageUp");
    await expect
      .poll(() => offset(chat.row, chat.viewport))
      .toBeGreaterThan(100);
    chat.reply(short);
    await expect(page.getByText(short)).toBeAttached();
    await page.setViewportSize({ width: 390, height: 754 });
    await expect
      .poll(() => offset(chat.row, chat.viewport))
      .toBeGreaterThan(100);
    await expect(chat.viewport).toBeFocused();
  } finally {
    await chat.close();
  }
});

test("the first admitted send survives new-chat URL replacement; reloading restores normal scrolling", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const human = { id: "first-human", type: "human", content: sent };
  const answer = {
    id: "msg-ai-1",
    type: "ai",
    content: "Hello from DeerFlow!",
  };
  mockLangGraphAPI(page, {
    createdThreadMessages: [human, answer],
    runStreamHandler: (route) =>
      handleRunStream(route, {}, [human], { responseMessage: answer }),
  });
  await page.goto("/workspace/chats/new", { waitUntil: "domcontentloaded" });
  const composer = page.getByPlaceholder(/how can i assist you/i);
  await composer.fill(sent);
  await composer.press("Enter");
  const viewport = page
    .getByTestId("main-message-list")
    .locator(":scope > div")
    .first();
  const row = page
    .getByText(sent, { exact: true })
    .locator("xpath=ancestor::*[@data-message-group-index][1]");
  await expect(page.getByText("Hello from DeerFlow!")).toBeVisible();
  await expect(page).not.toHaveURL(/\/new$/);
  await expect.poll(() => offset(row, viewport)).toBe(16);
  await page.reload({ waitUntil: "domcontentloaded" });
  await expect(composer).toBeVisible({ timeout: 15_000 });
  await expect(page.getByText("Hello from DeerFlow!")).toBeVisible();
  await expect
    .poll(() =>
      viewport.evaluate(
        (v) =>
          parseFloat(
            (v.firstElementChild?.lastElementChild as HTMLElement)?.style
              .marginBottom,
          ) || 0,
      ),
    )
    .toBe(0);
});
