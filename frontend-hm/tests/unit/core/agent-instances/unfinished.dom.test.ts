import { afterEach, expect, test } from "@rstest/core";

import {
  clearCreation,
  matchesCreation,
  readCreation,
  rememberCreation,
  type RememberedCreation,
} from "@/core/agent-instances/unfinished";

const request: RememberedCreation = {
  body: {
    creation_id: "a".repeat(32),
    name: "Researcher",
    definition_name: "research-aabbccdd",
    custody: "personal",
  },
  definition_name: "research-aabbccdd",
  copy_of: null,
};
afterEach(() => localStorage.clear());

test("remembers the exact creation body under its account's key", () => {
  expect(rememberCreation("alice", request)).toBe(true);
  expect(
    JSON.parse(
      localStorage.getItem("deerflow.agent-instances.unfinished.alice")!,
    ),
  ).toEqual(request);
  expect(readCreation("alice")).toEqual(request);
  expect(readCreation("bob")).toBeNull();
});
test("matches the unfinished entry by name and custody", () => {
  expect(
    matchesCreation(request, { name: "Researcher", custody: "personal" }),
  ).toBe(true);
  expect(
    matchesCreation(request, { name: "Researcher", custody: "company" }),
  ).toBe(false);
  expect(
    matchesCreation(request, { name: "Another", custody: "personal" }),
  ).toBe(false);
});
test("keeps a company copy's exact request and its source, with no added API fields", () => {
  const copy: RememberedCreation = {
    body: {
      creation_id: "b".repeat(32),
      generation: 7,
      name: "Research team",
      supervisor_id: "alice",
    },
    definition_name: null,
    copy_of: "c".repeat(32),
  };
  rememberCreation("alice", copy);
  expect(readCreation("alice")).toEqual(copy);
  expect(
    matchesCreation(copy, { name: "Research team", custody: "company" }),
  ).toBe(true);
  expect(
    matchesCreation(copy, { name: "Research team", custody: "personal" }),
  ).toBe(false);
});
test("clears only the owning account's creation", () => {
  rememberCreation("alice", request);
  rememberCreation("bob", request);
  expect(clearCreation("alice")).toBe(true);
  expect(readCreation("alice")).toBeNull();
  expect(readCreation("bob")).toEqual(request);
});
test.each([
  "invalid json",
  "null",
  "[]",
  "{}",
  JSON.stringify({ ...request, body: { ...request.body, creation_id: "bad" } }),
  JSON.stringify({ ...request, copy_of: "bad" }),
  JSON.stringify({
    ...request,
    body: {
      operation_id: "a".repeat(32),
      action: "delete",
      name: "Researcher",
      custody: "personal",
    },
  }),
])("ignores invalid or unrelated browser records", (raw) => {
  localStorage.setItem("deerflow.agent-instances.unfinished.alice", raw);
  expect(readCreation("alice")).toBeNull();
});
