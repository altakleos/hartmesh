import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { useState } from "react";

const state = rs.hoisted(() => ({ known: true, enabled: true }));
rs.mock("@/core/features", () => ({ useAgentInstances: () => state }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: "reader",
      system_role: "user",
      permissions: ["agents:read", "agents:write"],
    },
  }),
}));
rs.mock("@/core/agent-instances/api", { spy: true });
rs.mock("@/core/agents/api", () => ({ listAgents: rs.fn() }));

import {
  BriefStep,
  type BriefDraft,
} from "@/components/workspace/instances/brief-step";
import {
  listInstanceRoles,
  type Custody,
  type InstanceRole,
} from "@/core/agent-instances/api";
import { listAgents } from "@/core/agents/api";
import { I18nProvider } from "@/core/i18n/context";

const roles: InstanceRole[] = [
  {
    id: "research",
    name: "Research analyst",
    description: "Research topics",
    soul: "Research carefully",
    skills: ["research"],
    work_policy: null,
  },
  {
    id: "writer",
    name: "Writer",
    description: "Draft clear text",
    soul: "Write clearly",
    skills: ["writing"],
    work_policy: { enabled: true },
  },
];
const clients: QueryClient[] = [];
function Form({
  kind = "personal",
  initial,
}: {
  kind?: Custody;
  initial?: Partial<BriefDraft>;
}) {
  const [draft, setDraft] = useState<BriefDraft>({
    name: "",
    brief: "",
    role: null,
    useCustom: false,
    customDefinition: null,
    ...initial,
  });
  return <BriefStep kind={kind} value={draft} onChange={setDraft} />;
}
function show(props: React.ComponentProps<typeof Form> = {}) {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <Form {...props} />
      </I18nProvider>
    </QueryClientProvider>,
  );
}
beforeEach(() => {
  state.known = true;
  state.enabled = true;
  rs.mocked(listInstanceRoles).mockReset().mockResolvedValue({ roles });
  rs.mocked(listAgents).mockReset().mockResolvedValue([]);
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});

test("loads roles from the deployment and copies the selected role's brief and initial name", async () => {
  show();
  fireEvent.click(
    await screen.findByRole("button", {
      name: /Research analyst.*Research topics/,
    }),
  );
  expect(screen.getByLabelText<HTMLInputElement>("Name").value).toBe(
    "Research analyst",
  );
  expect(
    screen.getByRole<HTMLTextAreaElement>("textbox", {
      name: /responsible for/,
    }).value,
  ).toBe("Research carefully");
  expect(
    screen
      .getByRole("button", { name: /Research analyst.*Research topics/ })
      .getAttribute("aria-pressed"),
  ).toBe("true");
});
test("does not replace a name the person already supplied", async () => {
  show({ initial: { name: "My researcher" } });
  fireEvent.click(
    await screen.findByRole("button", {
      name: /Research analyst.*Research topics/,
    }),
  );
  expect(screen.getByLabelText<HTMLInputElement>("Name").value).toBe(
    "My researcher",
  );
});
test("asks before replacing an edited brief and supports both choices", async () => {
  show();
  fireEvent.click(
    await screen.findByRole("button", {
      name: /Research analyst.*Research topics/,
    }),
  );
  fireEvent.change(screen.getByRole("textbox", { name: /responsible for/ }), {
    target: { value: "My edited instructions" },
  });
  fireEvent.click(
    screen.getByRole("button", { name: /Writer.*Draft clear text/ }),
  );
  let dialog = screen.getByRole("dialog");
  fireEvent.click(
    within(dialog).getByRole("button", { name: "Keep your brief" }),
  );
  expect(
    screen.getByRole<HTMLTextAreaElement>("textbox", {
      name: /responsible for/,
    }).value,
  ).toBe("My edited instructions");
  fireEvent.click(
    screen.getByRole("button", { name: /Research analyst.*Research topics/ }),
  );
  dialog = screen.getByRole("dialog");
  fireEvent.click(
    within(dialog).getByRole("button", { name: "Use the role's brief" }),
  );
  expect(
    screen.getByRole<HTMLTextAreaElement>("textbox", {
      name: /responsible for/,
    }).value,
  ).toBe("Research carefully");
});
test.each(["personal", "company"] as const)(
  "qualifies the unnamed brief and role headings for %s",
  async (kind) => {
    rs.mocked(listInstanceRoles).mockResolvedValue({ roles: [] });
    show({ kind });
    expect(
      await screen.findByText(
        "No ready-made roles are set up. Describe the role below.",
      ),
    ).toBeDefined();
    expect(
      screen.getByText(
        kind === "personal"
          ? "What will this AI assistant do?"
          : "What will this AI employee do?",
      ),
    ).toBeDefined();
    expect(
      screen.getByRole("textbox", {
        name:
          kind === "personal"
            ? "What this AI assistant is responsible for"
            : "What this AI employee is responsible for",
      }),
    ).toBeDefined();
    expect(screen.getByRole("button", { name: /Blank brief/ })).toBeDefined();
  },
);
test("keeps the blank brief usable when roles fail and retries only on request", async () => {
  rs.mocked(listInstanceRoles).mockRejectedValueOnce(
    new Error("private diagnostic"),
  );
  show();
  expect(
    await screen.findByText(
      "The ready-made roles could not be loaded. Describe the role below, or try again.",
    ),
  ).toBeDefined();
  expect(screen.queryByText("private diagnostic")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Try again" }));
  await screen.findByRole("button", { name: /Research analyst/ });
  expect(listInstanceRoles).toHaveBeenCalledTimes(2);
});
test("switches to the person's custom agents without rewriting the brief", async () => {
  rs.mocked(listAgents).mockResolvedValue([
    {
      name: "custom-research",
      description: "My custom brief",
      model: null,
      tool_groups: null,
      skills: null,
    },
  ]);
  show({ initial: { name: "My researcher", brief: "Keep this draft" } });
  fireEvent.click(
    screen.getByRole("button", { name: "Use one of your custom agents" }),
  );
  const select = await screen.findByLabelText("Custom agent");
  await waitFor(() =>
    expect(
      within(select).getByRole("option", { name: "custom-research" }),
    ).toBeDefined(),
  );
  fireEvent.change(select, { target: { value: "custom-research" } });
  fireEvent.click(
    screen.getByRole("button", { name: "Describe the role instead" }),
  );
  expect(
    screen.getByRole<HTMLTextAreaElement>("textbox", {
      name: /responsible for/,
    }).value,
  ).toBe("Keep this draft");
});
test("does not offer a custom-agent lookup while discovery is unknown", () => {
  state.known = false;
  show();
  expect(
    screen.queryByRole("button", { name: "Use one of your custom agents" }),
  ).toBeNull();
  expect(listAgents).not.toHaveBeenCalled();
  expect(listInstanceRoles).not.toHaveBeenCalled();
});
test("shows localized validation for an empty or oversized brief and invalid name", async () => {
  show({ initial: { name: "x".repeat(129), brief: "x".repeat(16_001) } });
  expect(screen.getByText("Use a name of 1 to 128 characters.")).toBeDefined();
  expect(
    screen.getByText("The brief is too long. Keep it under 16,000 characters."),
  ).toBeDefined();
  fireEvent.change(screen.getByRole("textbox", { name: /responsible for/ }), {
    target: { value: "" },
  });
  expect(screen.getByText("Write a few words about the role.")).toBeDefined();
});
