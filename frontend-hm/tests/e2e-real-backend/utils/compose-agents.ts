/** Real browser UI over bounded HTTP fixtures; SQL/native authority stays in CI. */
import { expect, type Page } from "@playwright/test";

import type {
  AgentInstanceDetail,
  InstanceList,
  LifecycleChange,
  WorkRecord,
  WorkCapacity,
} from "../../../src/core/agent-instances/api";
import type { FeaturesResponse } from "../../../src/core/features/api";
import type { SpaceFile } from "../../../src/core/spaces/api";
import { MOCK_THREAD_ID, mockLangGraphAPI } from "../../e2e/utils/mock-api";

export { MOCK_THREAD_ID };
export const AGENT_ID = "a".repeat(32);
export const HOME_ID = "b".repeat(32);
export const WORK_ID = "c".repeat(32);
export const NOW = "2026-10-16T08:00:00Z";
export const COLLEAGUE = {
  id: "colleague",
  name: "Alex Rivera",
  email: "alex@example.test",
  active: true,
};
export function agent(
  overrides: Partial<AgentInstanceDetail> = {},
): AgentInstanceDetail {
  return {
    id: AGENT_ID,
    name: "Researcher",
    custody: "personal",
    owner_id: "default",
    creator_id: "default",
    supervisor: { kind: "human", subject_id: "default" },
    principal: { kind: "nonhuman", subject_id: `agent:${AGENT_ID}` },
    definition_revision: "d".repeat(64),
    generation: 1,
    status: "active",
    home_id: HOME_ID,
    permissions: 7,
    activity: {
      running_runs: 0,
      running: [],
      last_active_at: null,
      open_conversations: 1,
    },
    home: { state: "available", workspace: "absent" },
    execution: { execution: "ready", reason: null },
    ...overrides,
  };
}
export function assignment(overrides: Partial<WorkRecord> = {}): WorkRecord {
  return {
    id: WORK_ID,
    instance_id: AGENT_ID,
    creator_id: "default",
    definition_revision: "d".repeat(64),
    objective: "Review the guide",
    success_criteria: "List verified gaps",
    priority: "normal",
    review_required: true,
    assignment_revision: 1,
    revision: 1,
    status: "open",
    progress: "",
    next_action: "",
    sources: [],
    review_state: "none",
    outcome: null,
    review: null,
    blocker: null,
    attempt: null,
    execution_available: true,
    availability: "explicit_activation",
    work_enabled: true,
    needs_mandate_reconciliation: false,
    current_contents: "not_checked",
    created_at: NOW,
    updated_at: NOW,
    ...overrides,
  };
}
export function features(): FeaturesResponse {
  return {
    agents_api: { enabled: true },
    agent_instances: {
      enabled: true,
      execution: "ready",
      reason: null,
      ai_assistants: { per_user: 3, allowed: 8, in_use: 1, mine: 1 },
      ai_employees: { allowed: 4, in_use: 0 },
    },
    storage_spaces: {
      enabled: true,
      state: "ready",
      own: "ready",
      user_spaces: true,
    },
    people_search: "prefix",
    browser_control: { enabled: true },
    ui: { profile: "developer" },
    branding: {
      provider: {
        display_name: "Example Provider",
        support_url: "https://support.example.test/help",
      },
    },
  };
}
export async function setupAgents(
  page: Page,
  options: {
    instance?: AgentInstanceDetail;
    messages?: unknown[];
    empty?: boolean;
    role?: "admin" | "user";
  } = {},
) {
  page.setDefaultTimeout(10000);
  const state = {
    instance: options.instance ?? agent(),
    feature: features(),
    user: {
      id: "default",
      email: "owner@example.test",
      system_role: options.role ?? "admin",
      needs_setup: false,
    },
    visible: !options.empty,
    withdrawn: false,
    unfinished: [] as InstanceList["unfinished"],
    grants: [{ user_id: "default", permissions: 7 }],
    work: [] as WorkRecord[],
    capacity: undefined as WorkCapacity | undefined,
    history: [] as object[],
    lifecycle: [] as LifecycleChange[],
    creations: [] as Record<string, unknown>[],
    definitions: [] as Record<string, unknown>[],
    writes: [] as { path: string; method: string; operation: string | null }[],
    files: [] as SpaceFile[],
    uploadRefusal: null as null | "files-full" | "file-too-large",
    uploadGate: null as Promise<void> | null,
    pending: false,
    idleConflict: false,
    runs: 0,
    executionStarts: [] as { method: string; path: string }[],
    commands: [] as Record<string, unknown>[],
    humanInputs: [] as object[],
  };
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (
      request.method() === "POST" &&
      /\/(?:runs(?:\/stream)?|run|activate|resume|start|prepare|prewarm|attachments)$/.test(
        path,
      )
    ) {
      state.executionStarts.push({ method: request.method(), path });
    }
  });
  mockLangGraphAPI(page, {
    threads: [
      {
        thread_id: MOCK_THREAD_ID,
        title: "Guide discussion",
        agent_instance: {
          id: state.instance.id,
          name: state.instance.name,
          custody: state.instance.custody,
          status: state.instance.status,
          requester_id: "default",
        },
        messages: options.messages ?? [],
      },
    ],
  });
  await page.route("**/api/v1/auth/me", (route) =>
    route.fulfill({ json: state.user }),
  );
  await page.route("**/api/v1/auth/users**", (route) =>
    route.fulfill({
      json: {
        users: [
          COLLEAGUE,
          {
            id: "default",
            email: "owner@example.test",
            name: "Morgan Lee",
            active: true,
          },
        ],
      },
    }),
  );
  await page.route("**/api/features", (route) =>
    route.fulfill({ json: state.feature }),
  );
  function threadRow() {
    return {
      thread_id: MOCK_THREAD_ID,
      metadata: {},
      status: "idle",
      created_at: NOW,
      updated_at: NOW,
      values: { title: "Guide discussion" },
      agent_instance: {
        id: state.instance.id,
        name: state.instance.name,
        custody: state.instance.custody,
        status: state.instance.status,
        requester_id: "default",
      },
    };
  }
  await page.route(`**/api/langgraph/threads/${MOCK_THREAD_ID}`, (route) =>
    route.request().method() === "GET"
      ? route.fulfill({ json: threadRow() })
      : route.fallback(),
  );
  await page.route("**/api/langgraph/threads/search", (route) =>
    route.fulfill({ json: state.withdrawn ? [] : [threadRow()] }),
  );
  await page.route("**/api/agents**", (route) => {
    if (route.request().method() === "POST") {
      const body = route.request().postDataJSON() as Record<string, unknown>;
      state.definitions.push(body);
      return route.fulfill({ json: body });
    }
    return route.fulfill({ json: { agents: [] } });
  });
  await page.route("**/api/human-input**", (route) => {
    const path = new URL(route.request().url()).pathname;
    if (/\/(responses|history)$/.test(path))
      return route.fulfill({
        json: { responses: [], events: [], has_more: false },
      });
    if (state.humanInputs.length && path.endsWith("/" + "e".repeat(32)))
      return route.fulfill({ json: state.humanInputs[0] });
    return route.fulfill({
      json: {
        requests: state.withdrawn ? [] : state.humanInputs,
        counts: {
          pending: state.withdrawn ? 0 : state.humanInputs.length,
          routing: 0,
          answered: 0,
        },
        has_more: false,
      },
    });
  });
  await page.route("**/api/spaces**", (route) => {
    const url = new URL(route.request().url());
    const space = {
      id: HOME_ID,
      name: "Researcher files",
      backing_handle: "fixture",
      mode: "native",
      generation: 1,
      status: "active",
      permissions: 31,
      custody: { kind: "company", principal: null },
    };
    if (url.pathname.endsWith("/files")) {
      const dir = url.searchParams.get("path") ?? "";
      const files = dir
        ? state.files.filter(
            (f) =>
              f.path.startsWith(dir + "/") &&
              !f.path.slice(dir.length + 1).includes("/"),
          )
        : ["outputs", "uploads"].map((name) => ({
            name,
            path: name,
            kind: "directory",
            size: 0,
            modified: 0,
            accessible: true,
          }));
      return route.fulfill({ json: { files, path: dir } });
    }
    if (url.pathname.endsWith("/content"))
      return route.fulfill({
        body: "Fixture contents",
        contentType: "text/plain",
      });
    if (url.pathname.endsWith("/" + HOME_ID))
      return route.fulfill({ json: space });
    return route.fulfill({ json: { spaces: [space] } });
  });
  await page.route("**/api/agent-instances**", async (route) => {
    const request = route.request(),
      url = new URL(request.url()),
      path = url.pathname,
      post = request.method() === "POST";
    if (/\/(files|content)$/.test(path) && request.method() !== "GET") {
      state.writes.push({
        path,
        method: request.method(),
        operation: url.searchParams.get("operation_id"),
      });
      if (state.uploadGate) await state.uploadGate;
      if (state.uploadRefusal)
        return route.fulfill({
          status: state.uploadRefusal === "files-full" ? 507 : 413,
          json: { reason: state.uploadRefusal, detail: "PRIVATE DIAGNOSTIC" },
        });
      const filePath = url.searchParams.get("path") ?? "uploads/note.txt";
      state.files.push({
        name: filePath.split("/").at(-1)!,
        path: filePath,
        kind: "file",
        size: 5,
        modified: 1,
        accessible: true,
      });
      return route.fulfill({
        json: {
          path: filePath,
          sandbox_path: `/mnt/user-data/${filePath}`,
          size: 5,
          sha256: "f".repeat(64),
          deduplicated: false,
        },
      });
    }
    if (path.endsWith("/roles"))
      return route.fulfill({
        json: {
          roles: [
            {
              id: "research",
              name: "Research",
              description: "Explore questions and explain findings",
              soul: "Research carefully and explain evidence.",
              skills: [],
              work_policy: null,
            },
          ],
        },
      });
    if (path.endsWith("/lifecycle")) {
      if (!post)
        return route.fulfill({
          json: {
            operations: state.pending
              ? state.lifecycle.map((body) => ({
                  ...body,
                  complete: false,
                  retry: body,
                }))
              : [],
          },
        });
      const body = request.postDataJSON() as LifecycleChange;
      state.lifecycle.push(body);
      if (
        state.idleConflict &&
        body.only_if_idle &&
        state.instance.activity.running_runs > 0
      )
        return route.fulfill({
          status: 409,
          json: {
            reason: "work-in-flight",
            run_ids: state.instance.activity.running.map((r) => r.run_id),
          },
        });
      if (body.action === "grant")
        state.grants = [
          ...state.grants.filter((g) => g.user_id !== body.member_id),
          { user_id: body.member_id!, permissions: body.permissions! },
        ];
      const status =
        body.action === "suspend"
          ? "suspended"
          : body.action === "archive"
            ? "archived"
            : body.action === "delete"
              ? "deleted"
              : body.action === "restore"
                ? "active"
                : state.instance.status;
      state.instance = {
        ...state.instance,
        status,
        generation: state.instance.generation + 1,
      };
      if (status === "deleted") {
        state.visible = false;
        const counts = state.feature.agent_instances!;
        if (state.instance.custody === "personal") {
          counts.ai_assistants!.mine = 0;
          counts.ai_assistants!.in_use = 0;
        } else counts.ai_employees!.in_use = 0;
      }
      return route.fulfill({
        status: state.pending ? 202 : 200,
        json: {
          instance: state.instance,
          complete: !state.pending,
          operation_id: body.operation_id,
          space_released: !state.pending,
        },
      });
    }
    if (path.endsWith("/conversations"))
      return route.fulfill({
        json: post
          ? { thread_id: MOCK_THREAD_ID }
          : {
              conversations: [
                {
                  thread_id: MOCK_THREAD_ID,
                  title: "Guide discussion",
                  requester_id: "default",
                  updated_at: NOW,
                  running: false,
                },
              ],
            },
      });
    if (path.endsWith("/definition"))
      return route.fulfill({
        json: {
          revision: state.instance.definition_revision,
          config: {
            name: "research",
            work_policy: { enabled: true, review_required: true },
          },
          soul:
            typeof state.definitions.at(-1)?.soul === "string"
              ? state.definitions.at(-1)!.soul
              : "Research carefully and explain evidence.",
        },
      });
    if (path.endsWith("/grants"))
      return route.fulfill({ json: { grants: state.grants } });
    if (path.endsWith("/history"))
      return route.fulfill({ json: { events: state.history } });
    if (path.endsWith("/work"))
      return route.fulfill({
        json: {
          work: state.work,
          execution_available: true,
          availability: "explicit_activation",
          work_capacity: state.capacity,
        },
      });
    if (path.includes("/work/")) {
      const record = state.work.find((w) => path.includes(w.id));
      if (post && path.endsWith("/commands") && record) {
        const body = request.postDataJSON() as {
          action: string;
          expected_revision: number;
          expected_assignment_revision: number;
          operation_id: string;
        };
        state.commands.push(body);
        if (body.action === "stop" && record.attempt)
          record.attempt = { ...record.attempt, status: "cancelled" };
        if (body.action === "reconcile_mandate")
          record.needs_mandate_reconciliation = false;
        if (body.action === "cancel") {
          record.status = "cancelled";
          record.assignment_revision++;
        }
        record.revision++;
        state.history.unshift({
          id: `event-${state.history.length}`,
          actor_id: "default",
          actor_kind: "human",
          action: body.action,
          revision: record.revision,
          assignment_revision: record.assignment_revision,
          created_at: NOW,
          record: { ...record },
          note: null,
        });
      }
      if (post && path.endsWith("/activate")) state.runs++;
      return route.fulfill({ json: record });
    }
    if (path.endsWith("/memory"))
      return route.fulfill({ json: { facts: [], summaries: [] } });
    if (path.endsWith("/" + state.instance.id))
      return state.withdrawn
        ? route.fulfill({ status: 404, json: { detail: "Missing" } })
        : route.fulfill({ json: state.instance });
    if (post && path.endsWith("/agent-instances")) {
      const body = request.postDataJSON() as Record<string, unknown>;
      state.creations.push(body);
      state.instance = {
        ...state.instance,
        name: String(body.name),
        custody: body.custody as "personal" | "company",
        owner_id: body.custody === "personal" ? "default" : null,
        supervisor: {
          kind: "human",
          subject_id:
            typeof body.supervisor_id === "string"
              ? body.supervisor_id
              : "default",
        },
      };
      state.visible = true;
      state.unfinished = [];
      return route.fulfill({ status: 201, json: state.instance });
    }
    return route.fulfill({
      json: {
        instances:
          state.visible &&
          !state.withdrawn &&
          state.instance.custody === url.searchParams.get("custody")
            ? [state.instance]
            : [],
        ai_assistants: state.feature.agent_instances!.ai_assistants,
        unfinished: state.unfinished,
      },
    });
  });
  return state;
}
export async function detail(page: Page, tab: string) {
  await page.goto(`/workspace/instances?instance=${AGENT_ID}&tab=${tab}`);
  await expect(
    page.getByRole("heading", { name: "Researcher", exact: true }),
  ).toBeVisible();
}
export async function addBrief(page: Page, kind: "personal" | "company") {
  await page.goto(
    kind === "personal" ? "/workspace/assistants" : "/workspace/employees",
  );
  await page
    .getByRole("button", {
      name: kind === "personal" ? "Add AI assistant" : "Add AI employee",
      exact: true,
    })
    .click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("button", { name: /Research Explore/ }).click();
  await dialog.getByLabel("Name", { exact: true }).fill("Researcher");
  await dialog
    .locator("textarea")
    .fill("Research carefully. Ask before changing shared files.");
  return dialog;
}

/** Refresh the real AuthProvider after SSR starts with the auth-disabled fixture. */
export async function refreshAccount(page: Page) {
  const read = page.waitForResponse((response) =>
    response.url().endsWith("/api/v1/auth/me"),
  );
  await page.evaluate(() =>
    document.dispatchEvent(new Event("visibilitychange")),
  );
  await read;
}
