import { describe, expect, test } from "@rstest/core";

import {
  connectionDraft,
  formCanEdit,
  formErrors,
  toServer,
} from "@/core/mcp/connection-form";

const simple = {
  enabled: false,
  description: "Original",
  type: "http",
  url: "https://mcp.example.com/endpoint",
  headers: { authorization: "***" },
  routing: { mode: "off", priority: 0, keywords: [] },
  tools: {},
  task_toolsets: [],
  tool_name_prefix: false,
  tool_call_timeout: 12,
  session_init_timeout: null,
  unknown_option: { preserve: true },
};
describe("the connection form", () => {
  test("keeps the complete returned definition, header spelling and bare mask on edit", () => {
    expect(formCanEdit(simple)).toBe(true);
    const draft = {
      ...connectionDraft("Old name", simple),
      description: "New description",
    };
    expect(formErrors(draft, {}, true)).toEqual({});
    expect(toServer(draft, simple)).toEqual({
      ...simple,
      description: "New description",
    });
    expect(
      toServer({ ...draft, tokenChanged: true, token: "replacement" }, simple)
        .headers,
    ).toEqual({ authorization: "Bearer replacement" });
    expect(
      toServer({ ...draft, tokenChanged: true, token: "" }, simple).headers,
    ).toEqual({});
  });
  test("adds a remote connection without generating its identity", () => {
    const draft = {
      ...connectionDraft(),
      name: "project_notes",
      url: simple.url,
      description: "Shared notes",
    };
    expect(toServer(draft)).toEqual({
      type: "http",
      url: simple.url,
      description: "Shared notes",
      enabled: true,
      headers: {},
    });
    expect(
      toServer({ ...draft, type: "sse", token: "example" }).headers,
    ).toEqual({ Authorization: "Bearer example" });
  });
  test.each(["", "a".repeat(49), "Company notes", "$name"])(
    "refuses invalid names %s",
    (name) => {
      expect(
        formErrors({ ...connectionDraft(), name, url: simple.url }, {}, false)
          .name,
      ).toBeTruthy();
    },
  );
  test.each([
    "http://example.com",
    "https://",
    "https://name:secret@example.com",
    "$address",
  ])("refuses invalid addresses %s", (url) => {
    expect(
      formErrors({ ...connectionDraft(), name: "notes", url }, {}, false).url,
    ).toBeTruthy();
  });
  test.each([" leading", "trailing ", "two\nlines", "a\tb", "📝", "$key"])(
    "refuses invalid token %s",
    (token) => {
      expect(
        formErrors(
          { ...connectionDraft(), name: "notes", url: simple.url, token },
          {},
          false,
        ).token,
      ).toBeTruthy();
    },
  );
  test("rejects duplicate names and environment references in descriptions", () => {
    expect(
      formErrors(
        {
          ...connectionDraft(),
          name: "notes",
          url: simple.url,
          description: "$label",
        },
        { notes: simple },
        false,
      ),
    ).toEqual({ name: "duplicate", description: "literal" });
  });
  test.each([
    { type: "stdio" },
    { oauth: {} },
    { headers: { custom: "***" } },
    { user_auth: {} },
    { agent_auth: true },
    { headers_from_context: {} },
    { task_toolsets: [{}] },
    { routing: { mode: "prefer" } },
    { tools: { read: {} } },
  ])("retains the JSON editor for complex definitions", (overrides) => {
    expect(formCanEdit({ ...simple, ...overrides })).toBe(false);
  });
});
