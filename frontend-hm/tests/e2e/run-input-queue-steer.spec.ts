import { expect, test, type Route } from "@playwright/test";

import { mockLangGraphAPI, MOCK_THREAD_ID } from "./utils/mock-api";

const RUN_A = "00000000-0000-0000-0000-000000000091";
const RUN_B = "00000000-0000-0000-0000-000000000092";
const INPUT_TEXT = "Check the updated requirement";

type SubmittedInput = {
  request_id: string;
  text: string;
  mode: "queue" | "steer";
};

/**
 * A run that accepts Queue/Steer inputs is followed by the open chat even when
 * this page did not start it. A rejected input keeps the draft; the next send
 * targets the run then active, under a new request ID.
 */
for (const mode of ["queue", "steer"] as const) {
  test(`${mode} keeps a rejected draft and sends it to the next active run`, async ({
    page,
  }) => {
    let activeRunId = RUN_A;
    let acceptedInput: Record<string, unknown> | undefined;
    const submissions: Array<SubmittedInput & { runId: string }> = [];
    const streams = new Map<string, Route>();
    const run = (runId: string) => ({
      run_id: runId,
      thread_id: MOCK_THREAD_ID,
      assistant_id: "lead_agent",
      status: runId === activeRunId ? "running" : "success",
      metadata: {},
      kwargs: {},
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
    mockLangGraphAPI(page, {
      features: { runInputs: true },
      threads: [
        {
          thread_id: MOCK_THREAD_ID,
          title: "Queue and Steer",
          messages: [
            { type: "human", id: "human-original", content: "Start working" },
          ],
        },
      ],
    });
    await page.route(`**/api/threads/${MOCK_THREAD_ID}/inputs`, (route) =>
      route.fulfill({
        json: {
          items: acceptedInput ? [acceptedInput] : [],
          active_run_id: activeRunId,
        },
      }),
    );
    await page.route(
      new RegExp(
        `/api/langgraph/threads/${MOCK_THREAD_ID}/runs/(${RUN_A}|${RUN_B})(?:\\?|$)`,
      ),
      (route) => {
        const runId = new URL(route.request().url()).pathname
          .split("/")
          .at(-1)!;
        return route.fulfill({ json: run(runId) });
      },
    );
    // Hold each joined stream until the test finishes that run, so receiving
    // B's join request proves the page adopted the new run with its draft.
    await page.route(
      `**/api/langgraph/threads/${MOCK_THREAD_ID}/runs/*/stream*`,
      (route) => {
        const runId = new URL(route.request().url()).pathname
          .split("/")
          .at(-2)!;
        streams.set(runId, route);
      },
    );
    await page.route(
      `**/api/threads/${MOCK_THREAD_ID}/runs/*/inputs`,
      (route) => {
        const runId = new URL(route.request().url()).pathname
          .split("/")
          .at(-2)!;
        const body = route.request().postDataJSON() as SubmittedInput;
        submissions.push({ ...body, runId });
        if (runId === RUN_A)
          return route.fulfill({
            status: 409,
            json: { detail: "Run is finishing; this input was not accepted" },
          });
        acceptedInput = {
          id: body.request_id,
          target_run_id: runId,
          message_id: "human-update",
          text: body.text,
          mode: body.mode,
          status: "pending",
          created_at: "2026-01-01T00:00:01Z",
        };
        return route.fulfill({ json: acceptedInput });
      },
    );

    async function finishStream(runId: string) {
      const route = streams.get(runId);
      if (!route) return;
      streams.delete(runId);
      await route.fulfill({
        contentType: "text/event-stream",
        body:
          `event: metadata\ndata: ${JSON.stringify({ run_id: runId, thread_id: MOCK_THREAD_ID })}\n\n` +
          "event: end\ndata: {}\n\n",
      });
    }

    try {
      await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
      await expect.poll(() => streams.has(RUN_A)).toBe(true);
      const textarea = page.getByPlaceholder(/how can i assist you/i);
      const send = page.getByRole("button", { name: "Send update" });
      await expect(
        page.getByRole("button", { name: "Stop current task" }),
      ).toBeVisible();
      await textarea.fill(INPUT_TEXT);
      if (mode === "steer") {
        await page.getByRole("button", { name: "Send while working" }).click();
        await page.getByRole("menuitem", { name: /^Steer/ }).click();
      }
      await send.click();
      await expect(
        page.getByText(
          "The message was not confirmed. Check the task status and try again; your draft is kept.",
        ),
      ).toBeVisible();
      await expect(textarea).toHaveValue(INPUT_TEXT);
      expect(submissions).toHaveLength(1);
      expect(submissions[0]).toMatchObject({
        runId: RUN_A,
        text: INPUT_TEXT,
        mode,
      });

      activeRunId = RUN_B;
      await finishStream(RUN_A);
      await expect.poll(() => streams.has(RUN_B)).toBe(true);
      await send.click();
      await expect.poll(() => submissions.length).toBe(2);
      expect(submissions[1]).toMatchObject({
        runId: RUN_B,
        text: INPUT_TEXT,
        mode,
      });
      expect(submissions[1]!.request_id).not.toBe(submissions[0]!.request_id);
      await expect(textarea).toHaveValue("");
      await expect(
        page.getByTestId("run-inputs").getByText(INPUT_TEXT),
      ).toBeVisible();
    } finally {
      await Promise.all([...streams.keys()].map(finishStream));
    }
  });
}

test("without run_inputs the composer keeps waiting while a run streams", async ({
  page,
}) => {
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: "No Queue and Steer",
        messages: [{ type: "human", id: "human-1", content: "Start working" }],
      },
    ],
  });
  let inputReads = 0;
  await page.route(`**/api/threads/${MOCK_THREAD_ID}/inputs`, (route) => {
    inputReads += 1;
    return route.fulfill({ json: { items: [], active_run_id: RUN_A } });
  });
  await page.goto(`/workspace/chats/${MOCK_THREAD_ID}`);
  await expect(page.getByPlaceholder(/how can i assist you/i)).toBeVisible();
  await expect(page.getByRole("button", { name: "Send update" })).toHaveCount(
    0,
  );
  expect(inputReads).toBe(0);
});
