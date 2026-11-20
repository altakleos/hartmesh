import { expect, test } from "@rstest/core";

import {
  activeConnectionNames,
  type AgentConnection,
} from "@/core/agent-instances/connections";
import {
  selectAgentInstances,
  type FeaturesResponse,
} from "@/core/features/api";
test("exposes only usable names for manager or member projections", () => {
  const row = {
    id: "a",
    name: "notes",
    host: "notes.example",
    description: "",
    given: true,
    given_at: null,
    given_by: null,
    reason: null,
    can_give: true,
  };
  const roster: AgentConnection[] = [
    row,
    { ...row, name: "off", reason: "switched-off" },
    { ...row, name: "other", given: false },
    { ...row, name: "old", reason: "per-person-credentials" },
  ];
  expect(activeConnectionNames(roster)).toEqual(["notes"]);
  expect(
    activeConnectionNames([{ host: "archive.example", name: "Archive" }]),
  ).toEqual(["Archive"]);
});
test("requires a current affirmative connection feature for either surface", () => {
  const agents = { enabled: true, execution: "ready" };
  const feature = (value: typeof agents & { connections?: boolean }) =>
    selectAgentInstances({ agent_instances: value } as FeaturesResponse);
  expect(feature(agents).connections).toBeFalsy();
  expect(feature({ ...agents, connections: true }).connections).toBe(true);
  expect(
    feature({ ...agents, enabled: false, connections: true }).connections,
  ).toBeFalsy();
});
