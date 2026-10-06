import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, test } from "@rstest/core";
import ts from "typescript";

const root = process.cwd();

function roots(configPath: string) {
  const config = ts.readConfigFile(resolve(root, configPath), (name) =>
    ts.sys.readFile(name),
  );
  expect(config.error).toBeUndefined();
  const parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, root);
  expect(parsed.errors).toEqual([]);
  return parsed.fileNames.map((name) => name.replaceAll("\\", "/"));
}

test("production type checking excludes provider-dependent test fixtures", () => {
  const nextConfig = readFileSync(resolve(root, "next.config.js"), "utf8");
  const configPath =
    /tsconfigPath:\s*["']([^"']+)["']/.exec(nextConfig)?.[1] ?? "tsconfig.json";
  const files = roots(configPath);
  expect(files.some((name) => name.includes("/src/app/"))).toBe(true);
  expect(files.filter((name) => name.includes("/tests/"))).toEqual([]);
  expect(
    files.filter((name) =>
      /\/(rstest|playwright)[^/]*\.config\.ts$/.test(name),
    ),
  ).toEqual([]);
});

test("developer type checking still includes the actual provider test helpers", () => {
  const files = roots("tsconfig.json");
  expect(files).toContain(
    resolve(root, "tests/helpers/legacy-report-card.tsx").replaceAll("\\", "/"),
  );
  expect(files).toContain(
    resolve(root, "tests/e2e/utils/legacy-report-plugin.ts").replaceAll(
      "\\",
      "/",
    ),
  );
});
