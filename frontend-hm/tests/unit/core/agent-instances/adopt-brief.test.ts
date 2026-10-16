import { beforeEach, expect, test, rs } from "@rstest/core";

rs.mock("@/core/agents/api", () => {
  const actual = rs.requireActual<typeof definitionAPI>("@/core/agents/api");
  return {
    ...actual,
    getAgent: rs.fn(),
    createAgent: rs.fn(),
    updateAgent: rs.fn(),
  };
});

import { prepareBriefDefinition } from "@/core/agent-instances/adopt-brief";
import type * as definitionAPI from "@/core/agents/api";
import {
  AgentDefinitionError,
  createAgent,
  getAgent,
  updateAgent,
} from "@/core/agents/api";
import type { Agent, CreateAgentRequest } from "@/core/agents/types";

const config: CreateAgentRequest & { memory_enabled?: boolean } = {
  name: "researcher",
  description: "Research",
  model: "model",
  tool_groups: ["web"],
  skills: ["research"],
  allowed_subagents: ["analyst"],
  model_settings: { temperature: 0.2, max_tokens: 2000 },
  thinking_enabled: true,
  reasoning_effort: "high",
  work_policy: {
    enabled: true,
    review_required: false,
    allow_derived: true,
    responsibilities: [{ key: "research", label: "Research" }],
  },
  memory_enabled: false,
};
beforeEach(() => {
  rs.mocked(getAgent)
    .mockReset()
    .mockResolvedValue({ name: "researcher" } as Agent);
  rs.mocked(updateAgent)
    .mockReset()
    .mockResolvedValue({ name: "researcher" } as Agent);
  rs.mocked(createAgent)
    .mockReset()
    .mockResolvedValue({ name: "researcher" } as Agent);
});
test("updates the actor-owned source preserving every supported field and excluding projections", async () => {
  const owner = new AbortController();
  await prepareBriefDefinition(config, "Changed brief", false, owner.signal);
  expect(updateAgent).toHaveBeenCalledWith(
    "researcher",
    {
      ...config,
      memory_enabled: undefined,
      name: undefined,
      soul: "Changed brief",
      work_policy: { ...config.work_policy, enabled: false },
    },
    owner.signal,
  );
  const body = rs.mocked(updateAgent).mock.calls[0]![1];
  expect(body).not.toHaveProperty("memory_enabled");
  expect(body).not.toHaveProperty("name");
  expect(createAgent).not.toHaveBeenCalled();
});
test("creates a source only after an authoritative missing response and uses the default Work policy", async () => {
  rs.mocked(getAgent).mockRejectedValue(
    new AgentDefinitionError(404, "missing"),
  );
  const owner = new AbortController();
  await prepareBriefDefinition(
    { name: "researcher", work_policy: null },
    "Brief",
    false,
    owner.signal,
  );
  expect(createAgent).toHaveBeenCalledWith(
    {
      name: "researcher",
      soul: "Brief",
      work_policy: {
        enabled: false,
        default_priority: "normal",
        review_required: true,
        allow_derived: false,
        max_derived_per_activation: 3,
        responsibilities: [],
      },
    },
    owner.signal,
  );
  expect(updateAgent).not.toHaveBeenCalled();
});
test.each([403, 409, 503])(
  "a %s source read cannot create or update any definition",
  async (status) => {
    rs.mocked(getAgent).mockRejectedValue(
      new AgentDefinitionError(status, "private detail"),
    );
    await expect(
      prepareBriefDefinition(
        config,
        "Brief",
        true,
        new AbortController().signal,
      ),
    ).rejects.toMatchObject({ status });
    expect(createAgent).not.toHaveBeenCalled();
    expect(updateAgent).not.toHaveBeenCalled();
  },
);
test("retirement between source lookup and save prevents a follow-up mutation", async () => {
  const owner = new AbortController();
  rs.mocked(getAgent).mockImplementation(async () => {
    owner.abort();
    return { name: "researcher" } as Agent;
  });
  await expect(
    prepareBriefDefinition(config, "Brief", true, owner.signal),
  ).rejects.toMatchObject({ name: "AbortError" });
  expect(updateAgent).not.toHaveBeenCalled();
  expect(createAgent).not.toHaveBeenCalled();
});
