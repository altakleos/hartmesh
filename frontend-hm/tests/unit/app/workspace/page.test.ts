import { beforeEach, describe, expect, it, rs } from "@rstest/core";

const navigation = rs.hoisted(() => ({ redirect: rs.fn() }));
const environment = rs.hoisted(() => ({ staticOnly: "false" }));
rs.mock("next/navigation", () => ({ redirect: navigation.redirect }));
rs.mock("@/env", () => ({
  env: {
    get NEXT_PUBLIC_STATIC_WEBSITE_ONLY() {
      return environment.staticOnly;
    },
  },
}));

import WorkspacePage from "@/app/workspace/page";

const open = (query: Record<string, string | string[] | undefined>) =>
  WorkspacePage({ searchParams: Promise.resolve(query) });

beforeEach(() => {
  navigation.redirect.mockReset();
  environment.staticOnly = "false";
});

describe("the workspace page", () => {
  it("goes to a new chat", async () => {
    await open({});
    expect(navigation.redirect).toHaveBeenCalledWith("/workspace/chats/new");
  });

  it("keeps the query a bot's link or the sign-in return carries", async () => {
    await open({ settings: "channels", telegram: "custom_agent.helper" });
    expect(navigation.redirect).toHaveBeenCalledWith(
      "/workspace/chats/new?settings=channels&telegram=custom_agent.helper",
    );
  });

  it("keeps a repeated parameter and escapes what it carries", async () => {
    await open({ a: ["1", "2"], b: "x y&z" });
    expect(navigation.redirect).toHaveBeenCalledWith(
      "/workspace/chats/new?a=1&a=2&b=x+y%26z",
    );
  });
});
