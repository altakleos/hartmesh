import { execFileSync } from "node:child_process";
import { resolve } from "node:path";

import { expect, test } from "@rstest/core";
import ts from "typescript";

const root = process.cwd();

function nextBuildConfig(overrides: Record<string, string> = {}) {
  return JSON.parse(
    execFileSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        'import config from "./next.config.js"; console.log(JSON.stringify({ distDir: config.distDir, tsconfigPath: config.typescript.tsconfigPath }));',
      ],
      {
        cwd: root,
        encoding: "utf8",
        env: {
          ...process.env,
          SKIP_ENV_VALIDATION: "1",
          NEXT_DIST_DIR: "",
          NEXT_TSCONFIG_PATH: "",
          ...overrides,
        },
      },
    ),
  ) as { distDir: string; tsconfigPath: string };
}

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
  const config = nextBuildConfig();
  expect(config.distDir).toBe(".next");
  expect(config.tsconfigPath).toBe("tsconfig.build.json");
  const files = roots(config.tsconfigPath);
  expect(files.some((name) => name.includes("/src/app/"))).toBe(true);
  expect(files.filter((name) => name.includes("/tests/"))).toEqual([]);
  expect(
    files.filter((name) =>
      /\/(rstest|playwright)[^/]*\.config\.ts$/.test(name),
    ),
  ).toEqual([]);
});

test("isolated browser builds use their own output and type configuration", () => {
  expect(
    nextBuildConfig({
      NEXT_DIST_DIR: ".next-hartmesh-browser",
      NEXT_TSCONFIG_PATH: ".hartmesh-test/browser.json",
    }),
  ).toEqual({
    distDir: ".next-hartmesh-browser",
    tsconfigPath: ".hartmesh-test/browser.json",
  });
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
