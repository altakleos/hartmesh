import { expect, test } from "@rstest/core";

import { aboutMarkdown } from "@/components/workspace/settings/about-content";
import { APP_VERSION } from "@/version";

test("About is the name and the version, and nothing else", () => {
  const markdown = aboutMarkdown("Example Services Co.", "9.9.9-test");
  expect(markdown).toBe(
    "# Example Services Co.\n\nWorkspace version 9.9.9-test.\n",
  );
  expect(markdown).not.toContain("DeerFlow");
  expect(markdown).not.toContain("http");
});

test("a name is shown as typed, not read as markup", () => {
  expect(aboutMarkdown("C# Solutions [Bracket] Co.", "1")).toContain(
    "# C\\# Solutions \\[Bracket\\] Co.",
  );
});

test("About carries the resolved version by default", () => {
  expect(aboutMarkdown("HartMesh")).toContain(
    `Workspace version ${APP_VERSION}.`,
  );
});
