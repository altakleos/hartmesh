import { execFileSync } from "node:child_process";

import { expect, test } from "@rstest/core";

// The product renders no next/image, and /_next/image is reachable without
// signing in, so the Image Optimization API stays off.
test("the Next.js image optimizer is disabled", () => {
  const images = JSON.parse(
    execFileSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        'import config from "./next.config.js"; console.log(JSON.stringify(config.images ?? null));',
      ],
      {
        cwd: process.cwd(),
        encoding: "utf8",
        env: { ...process.env, SKIP_ENV_VALIDATION: "1" },
      },
    ),
  ) as { unoptimized?: boolean } | null;

  expect(images?.unoptimized).toBe(true);
});
